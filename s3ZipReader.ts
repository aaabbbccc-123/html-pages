// npm install yauzl @aws-sdk/client-s3
// npm install --save-dev @types/yauzl

import './polyfill';
import yauzl from 'yauzl';
import { S3Client, HeadObjectCommand, GetObjectCommand } from '@aws-sdk/client-s3';
import { PassThrough, Readable } from 'stream';

const NON_SEQUENTIAL_FETCH_CAP = 256 * 1024;

function chooseReadRangeEnd(
  position: number,
  requestLength: number,
  totalSize: number,
): number {
  return Math.min(
    totalSize,
    position + Math.max(requestLength, NON_SEQUENTIAL_FETCH_CAP),
  );
}

function collectReadable(stream: Readable): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    stream.on('data', (c: Buffer) => chunks.push(c));
    stream.on('end', () => resolve(Buffer.concat(chunks)));
    stream.on('error', reject);
  });
}


// ---------------------------------------------------------------------------
// Internal readers
// ---------------------------------------------------------------------------

abstract class CachedRandomAccessReader extends yauzl.RandomAccessReader {
  private cacheStart = -1;
  private cache: Buffer | null = null;

  constructor(
    private readonly totalSize: number,
  ) {
    super();
  }

  abstract readRanged(start: number, end: number): Promise<Readable>;
  /**
   * Buffered reads: yauzl issues many small sequential reads for the central directory.
   * Default RandomAccessReader opens one HTTP/S3 stream per read; we prefetch a larger range.
   */
  override read(
    buffer: Buffer,
    offset: number,
    length: number,
    position: number,
    callback: (err: Error | null) => void,
  ): void {
    if (length === 0) {
      setImmediate(() => callback(null));
      return;
    }
    const readEnd = position + length;
    if (
      this.cache !== null &&
      this.cacheStart <= position &&
      readEnd <= this.cacheStart + this.cache.length
    ) {
      this.cache.copy(buffer, offset, position - this.cacheStart, readEnd - this.cacheStart);
      setImmediate(() => callback(null));
      return;
    }
    const rangeEnd = chooseReadRangeEnd(
      position,
      length,
      this.totalSize,
    );
    this.readRanged(position, rangeEnd).
      then(async (readable) => {
        const data = await collectReadable(readable);
        this.cacheStart = position;
        this.cache = data;
        if (readEnd > this.cacheStart + this.cache.length) {
          throw new Error('unexpected EOF');
        }
        this.cache.copy(buffer, offset, 0, length);
        callback(null);
      })
      .catch(err => callback(err));
  }

  override _readStreamForRange(start: number, end: number): PassThrough {
    const pass = new PassThrough();
    if (
      this.cache !== null &&
      this.cacheStart <= start &&
      end <= this.cacheStart + this.cache.length
    ) {
      const slice = this.cache.subarray(start - this.cacheStart, end - this.cacheStart);
      setImmediate(() => pass.end(slice));
      return pass;
    }
    this.readRanged(start, end)
      .then(readable => readable.pipe(pass))
      .catch(err => pass.destroy(err));
    return pass;
  }
}


class S3RandomAccessReader extends CachedRandomAccessReader {

  constructor(
    private readonly s3: S3Client,
    private readonly bucket: string,
    private readonly key: string,
    totalSize: number,
  ) {
    super(totalSize);
  }

  override async readRanged(start: number, end: number): Promise<Readable> {
    return (this.s3
      .send(
        new GetObjectCommand({
          Bucket: this.bucket,
          Key: this.key,
          Range: `bytes=${start}-${end - 1}`,
        }),
      ).then(({ Body }) => Body as Readable));
  }
}

class HttpRandomAccessReader extends CachedRandomAccessReader {
  constructor(
    private readonly url: string,
    totalSize: number,
  ) {
    super(totalSize);
  }

  override async readRanged(start: number, end: number): Promise<Readable> {
    console.log(`fetching range ${start}-${end - 1}`);
    return fetch(this.url, {
      headers: { Range: `bytes=${start}-${end - 1}` },
    })
      .then(res => {
        if (!res.ok) throw new Error(`HTTP ${res.status} fetching range ${start}-${end - 1}`);
        return Readable.fromWeb(res.body as ReadableStream);
      });
  }
}

// ---------------------------------------------------------------------------
// Public class
// ---------------------------------------------------------------------------

export class S3ZipReader {
  /** First occurrence wins if the archive lists the same path twice. */
  private readonly entryByFileName = new Map<string, yauzl.Entry>();

  constructor(private readonly zipfile: yauzl.ZipFile) {
  }

  // -------------------------------------------------------------------------
  // Factories
  // -------------------------------------------------------------------------

  /**
   * Open the ZIP and load the central directory (one pass). Safe to call `listFiles` / `readFile` immediately.
   *
   * Supported URL shapes:
   *   s3://bucket/key
   *   https://bucket.s3.amazonaws.com/key
   *   https://bucket.s3.region.amazonaws.com/key
   *   https://s3.amazonaws.com/bucket/key
   */
  static async fromS3Url(url: string): Promise<S3ZipReader> {
    const { bucket, key } = S3ZipReader.parseS3Url(url);
    const s3 = new S3Client();
    const { ContentLength } = await s3.send(
      new HeadObjectCommand({ Bucket: bucket, Key: key })
    );

    const reader = new S3RandomAccessReader(s3, bucket, key, ContentLength!);
    const zipfile = await S3ZipReader.fromRandomAccessReader(reader, ContentLength!);
    const zipReader = new S3ZipReader(zipfile);
    await zipReader.scanCentralDirectory();
    return zipReader;
  }

  /**
   * Open from a pre-signed (or publicly accessible) HTTPS URL and load the central directory.
   * No AWS credentials are required at runtime.
   */
  static async fromWebUrl(url: string): Promise<S3ZipReader> {
    const head = await fetch(url, { headers: { Range: 'bytes=0-0' } });
    if (!head.ok) throw new Error(`GET with range 0-0 failed: HTTP ${head.status}`);

    const fileSize = parseInt(head.headers.get('Content-Range')?.split('/')[1] ?? '', 10);
    if (!fileSize) throw new Error('total size missing from Content-Range response');

    const reader = new HttpRandomAccessReader(url, fileSize);
    const zipfile = await S3ZipReader.fromRandomAccessReader(reader, fileSize);
    const zipReader = new S3ZipReader(zipfile);
    await zipReader.scanCentralDirectory();
    return zipReader;
  }

  // -------------------------------------------------------------------------
  // Public methods
  // -------------------------------------------------------------------------

  /** List all entries (served from the in-memory catalog). */
  listFiles(): string[] {
   return Array.from(this.entryByFileName.keys());
  }

  /**
   * Read a file's content from the ZIP.
   * Only the compressed bytes for that specific entry are fetched.
   */
  async readFile(fileName: string): Promise<Buffer> {
    if (!this.zipfile) throw new Error('ZIP is not open');
    const zipfile = this.zipfile;
    
    const entry = this.entryByFileName.get(fileName);

    if (!entry) throw new Error(`File not found in ZIP: ${fileName}`);
    if (entry.fileName.endsWith('/')) throw new Error(`"${fileName}" is a directory`);

    return new Promise((resolve, reject) => {
      zipfile.openReadStream(entry, (err, stream) => {
        if (err) return reject(err);
        const chunks: Buffer[] = [];
        stream.on('data', (chunk: Buffer) => chunks.push(chunk));
        stream.on('end', () => resolve(Buffer.concat(chunks)));
        stream.on('error', reject);
      });
    });
  }

  /** Close the underlying ZIP handle and drop cached metadata. */
  close(): void {
    this.zipfile?.close();
    this.entryByFileName.clear();
  }


  private async scanCentralDirectory(): Promise<void> {
    if (!this.zipfile) throw new Error('ZIP is not open');
    const zipfile = this.zipfile;
    this.entryByFileName.clear();

    await new Promise<void>((resolve, reject) => {
      const onEntry = (entry: yauzl.Entry) => {
        if (!entry.fileName.endsWith('/') && !this.entryByFileName.has(entry.fileName)) {
          this.entryByFileName.set(entry.fileName, entry);
        }
        zipfile.readEntry();
      };

      const onEnd = () => {
        zipfile.removeListener('entry', onEntry);
        zipfile.removeListener('error', onError);
        resolve();
      };

      const onError = (err: Error) => {
        zipfile.removeListener('entry', onEntry);
        zipfile.removeListener('end', onEnd);
        reject(err);
      };

      zipfile.on('entry', onEntry);
      zipfile.once('end', onEnd);
      zipfile.once('error', onError);
      zipfile.readEntry();
    });
  }


  private static async fromRandomAccessReader(
    reader: yauzl.RandomAccessReader,
    fileSize: number,
  ): Promise<yauzl.ZipFile> {
    return new Promise((resolve, reject) =>
      yauzl.fromRandomAccessReader(
        reader,
        fileSize,
        { lazyEntries: true, autoClose: false },
        (err, zip) => (err ? reject(err) : resolve(zip)),
      )
    );
  }

  private static parseS3Url(url: string): { bucket: string; key: string } {
    const s3Proto = url.match(/^s3:\/\/([^/]+)\/(.+)$/);
    if (s3Proto) return { bucket: s3Proto[1], key: s3Proto[2] };

    const parsed = new URL(url);

    const vHosted = parsed.hostname.match(/^(.+?)\.s3(?:\.[^.]+)?\.amazonaws\.com$/);
    if (vHosted) return { bucket: vHosted[1], key: parsed.pathname.replace(/^\//, '') };

    const pathStyle = parsed.hostname.match(/^s3(?:\.[^.]+)?\.amazonaws\.com$/);
    if (pathStyle) {
      const [, bucket, ...rest] = parsed.pathname.split('/');
      return { bucket, key: rest.join('/') };
    }

    throw new Error(`Unrecognised S3 URL format: ${url}`);
  }
}
