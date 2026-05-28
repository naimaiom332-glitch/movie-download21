// ============================================================
// DriveGet Backend — Node.js
// Google Drive → Cloudflare R2 Upload Server
// ============================================================

const express = require('express');
const cors = require('cors');
const { S3Client, PutObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const { google } = require('googleapis');
const axios = require('axios');
const { v4: uuidv4 } = require('uuid');
require('dotenv').config();

const app = express();
app.use(cors());
app.use(express.json());

// ============================================================
// CONFIG — .env ফাইলে এগুলো দাও
// ============================================================
const R2 = new S3Client({
  region: 'auto',
  endpoint: `https://${process.env.CF_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  credentials: {
    accessKeyId: process.env.R2_ACCESS_KEY_ID,
    secretAccessKey: process.env.R2_SECRET_ACCESS_KEY,
  },
});

const BUCKET_NAME = process.env.R2_BUCKET_NAME;
const R2_PUBLIC_URL = process.env.R2_PUBLIC_URL; // e.g. https://pub-xxx.r2.dev

// Google Drive API (optional, for better file info)
// যদি না থাকে তাহলে public URL থেকে সরাসরি ডাউনলোড করবে
const GDRIVE_API_KEY = process.env.GDRIVE_API_KEY || null;

// Jobs store (production এ Redis ব্যবহার করো)
const jobs = new Map();
// ============================================================

// ============================================================
// HELPER: Get Google Drive file info
// ============================================================
async function getFileInfo(fileId) {
  // Method 1: Google Drive API দিয়ে (API key থাকলে)
  if (GDRIVE_API_KEY) {
    try {
      const url = `https://www.googleapis.com/drive/v3/files/${fileId}?fields=name,size,mimeType,createdTime&key=${GDRIVE_API_KEY}`;
      const res = await axios.get(url);
      return res.data;
    } catch (e) {
      console.log('Drive API failed, trying HEAD request...');
    }
  }

  // Method 2: Direct HEAD request
  try {
    const downloadUrl = `https://drive.google.com/uc?export=download&id=${fileId}`;
    const res = await axios.head(downloadUrl, {
      maxRedirects: 5,
      headers: { 'User-Agent': 'Mozilla/5.0' },
    });

    const contentDisposition = res.headers['content-disposition'] || '';
    const nameMatch = contentDisposition.match(/filename[^;=\n]*=["']?([^"'\n;]+)/i);
    const name = nameMatch ? decodeURIComponent(nameMatch[1]) : `file_${fileId}`;
    const size = parseInt(res.headers['content-length'] || 0);
    const mimeType = res.headers['content-type'] || 'application/octet-stream';

    return { name, size, mimeType, createdTime: new Date().toISOString() };
  } catch (e) {
    return {
      name: `file_${fileId}`,
      size: 0,
      mimeType: 'application/octet-stream',
      createdTime: new Date().toISOString(),
    };
  }
}

// ============================================================
// HELPER: Download from GDrive & Upload to R2
// ============================================================
async function downloadAndUpload(fileId, jobId) {
  const job = jobs.get(jobId);
  if (!job) return;

  try {
    job.status = 'downloading';
    job.statusText = 'Downloading from Google Drive...';
    job.progress = 5;

    // Google Drive download URL
    const downloadUrl = `https://drive.google.com/uc?export=download&id=${fileId}&confirm=t`;

    // Download with stream
    const response = await axios({
      method: 'GET',
      url: downloadUrl,
      responseType: 'stream',
      maxRedirects: 10,
      headers: {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
      },
      onDownloadProgress: (progressEvent) => {
        // axios stream doesn't give this but we track manually below
      },
    });

    // Handle Google's virus scan warning redirect
    // (for large files Google shows a confirmation page)
    let finalStream = response.data;
    const contentType = response.headers['content-type'] || '';
    
    if (contentType.includes('text/html')) {
      // Need to handle confirmation
      job.statusText = 'Handling Drive confirmation...';
      // Re-fetch with confirmation token
      const confirmUrl = `https://drive.google.com/uc?export=download&id=${fileId}&confirm=t&uuid=${uuidv4()}`;
      const confirmRes = await axios({
        method: 'GET',
        url: confirmUrl,
        responseType: 'stream',
        maxRedirects: 10,
        headers: {
          'User-Agent': 'Mozilla/5.0',
          'Cookie': `download_warning_${fileId}=t`,
        },
      });
      finalStream = confirmRes.data;
    }

    const totalSize = job.fileSize || parseInt(response.headers['content-length'] || 0);
    let transferred = 0;
    let lastTime = Date.now();
    let lastTransferred = 0;

    job.status = 'uploading';
    job.statusText = 'Uploading to Cloudflare R2...';
    job.progress = 10;

    // Collect chunks for R2 upload
    // (For large files, use multipart upload — see comments below)
    const chunks = [];

    await new Promise((resolve, reject) => {
      finalStream.on('data', (chunk) => {
        chunks.push(chunk);
        transferred += chunk.length;

        // Calculate speed & progress
        const now = Date.now();
        const timeDiff = (now - lastTime) / 1000;
        if (timeDiff >= 0.5) {
          const bytesDiff = transferred - lastTransferred;
          const speed = bytesDiff / timeDiff;
          const eta = speed > 0 ? Math.round((totalSize - transferred) / speed) : 0;
          const progress = totalSize > 0
            ? Math.min(90, Math.round((transferred / totalSize) * 90))
            : Math.min(90, job.progress + 2);

          job.transferred = transferred;
          job.speed = speed;
          job.progress = progress;
          job.statusText = progress < 50 ? 'Downloading...' : 'Processing...';
          job.eta = eta > 0 ? `${eta}s` : '—';

          lastTime = now;
          lastTransferred = transferred;
        }
      });

      finalStream.on('end', resolve);
      finalStream.on('error', reject);
    });

    // Upload to R2
    job.statusText = 'Saving to R2 Storage...';
    job.progress = 92;

    const buffer = Buffer.concat(chunks);
    const fileName = job.fileName || `file_${fileId}`;
    const key = encodeURIComponent(fileName);

    await R2.send(new PutObjectCommand({
      Bucket: BUCKET_NAME,
      Key: key,
      Body: buffer,
      ContentType: job.mimeType || 'application/octet-stream',
      ContentDisposition: `attachment; filename="${fileName}"`,
    }));

    // Done!
    const r2Url = `${R2_PUBLIC_URL}/${key}`;
    job.status = 'done';
    job.statusText = 'Complete!';
    job.progress = 100;
    job.transferred = buffer.length;
    job.r2Url = r2Url;
    job.speed = 0;
    job.eta = '0s';

    console.log(`✅ Job ${jobId} done → ${r2Url}`);

  } catch (err) {
    console.error(`❌ Job ${jobId} error:`, err.message);
    job.status = 'error';
    job.statusText = 'Error';
    job.error = err.message;
  }
}

// ============================================================
// ROUTES
// ============================================================

// Health check
app.get('/', (req, res) => {
  res.json({ status: 'ok', service: 'DriveGet Backend' });
});

// GET file info
app.post('/api/info', async (req, res) => {
  const { fileId } = req.body;
  if (!fileId) return res.status(400).json({ error: 'fileId required' });

  try {
    const info = await getFileInfo(fileId);
    res.json(info);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

// Start upload job
app.post('/api/upload', async (req, res) => {
  const { fileId } = req.body;
  if (!fileId) return res.status(400).json({ error: 'fileId required' });

  // Check if already in R2
  const info = await getFileInfo(fileId);
  const key = encodeURIComponent(info.name || `file_${fileId}`);

  try {
    await R2.send(new HeadObjectCommand({ Bucket: BUCKET_NAME, Key: key }));
    // File already exists in R2!
    const r2Url = `${R2_PUBLIC_URL}/${key}`;
    const jobId = uuidv4();
    jobs.set(jobId, {
      status: 'done',
      statusText: 'Already in cache!',
      progress: 100,
      transferred: info.size || 0,
      fileSize: info.size || 0,
      fileName: info.name,
      mimeType: info.mimeType,
      r2Url,
      speed: 0,
      eta: '0s',
    });
    return res.json({ jobId, cached: true });
  } catch (e) {
    // Not in R2 yet, proceed with upload
  }

  const jobId = uuidv4();
  jobs.set(jobId, {
    status: 'queued',
    statusText: 'Queued Started',
    progress: 0,
    transferred: 0,
    fileSize: info.size || 0,
    fileName: info.name,
    mimeType: info.mimeType,
    speed: 0,
    eta: 'Unknown',
  });

  // Start upload in background
  downloadAndUpload(fileId, jobId).catch(console.error);

  res.json({ jobId, cached: false });
});

// Poll job status
app.get('/api/status/:jobId', (req, res) => {
  const job = jobs.get(req.params.jobId);
  if (!job) return res.status(404).json({ error: 'Job not found' });
  res.json(job);
});

// Share link redirect
app.get('/share/:fileId', async (req, res) => {
  const { fileId } = req.params;
  const info = await getFileInfo(fileId);
  const key = encodeURIComponent(info.name || fileId);
  const r2Url = `${R2_PUBLIC_URL}/${key}`;
  res.redirect(r2Url);
});

// ============================================================
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log(`🚀 DriveGet Backend running on port ${PORT}`);
});

module.exports = app;
