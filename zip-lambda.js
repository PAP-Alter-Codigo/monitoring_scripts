const fs = require('fs');
const archiver = require('archiver');

const output = fs.createWriteStream('lambda.zip');
const archive = archiver('zip', { zlib: { level: 9 } });

// List all files and folders you want to include
const filesToInclude = [
  'index.js',
  'package.json',
  'models/',
  'node_modules/'
];

output.on('close', () => {
  console.log(`lambda.zip created (${archive.pointer()} total bytes)`);
});

archive.on('error', err => { throw err; });

archive.pipe(output);

filesToInclude.forEach(item => {
  if (fs.existsSync(item)) {
    const stats = fs.statSync(item);
    if (stats.isDirectory()) {
      archive.directory(item, item);
    } else {
      archive.file(item, { name: item });
    }
  }
});

archive.finalize();