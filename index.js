console.log("🚀 FILE LOADED");

const sharp = require("sharp");
const {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} = require("@aws-sdk/client-s3");

const { MongoClient } = require("mongodb");

/* ---------------------------------------
CONFIG
--------------------------------------- */
const REGION = process.env.AWS_REGIONN; // ✅ FIXED
const BUCKET = process.env.S3_BUCKET;
const MONGO_URI = process.env.MONGO_URI;

const s3 = new S3Client({ region: REGION });

/* ---------------------------------------
DB
--------------------------------------- */
const client = new MongoClient(MONGO_URI);
let db;

async function getDb() {
  if (!db) {
    await client.connect();
    db = client.db();
  }
  return db;
}

/* ---------------------------------------
HELPERS
--------------------------------------- */
async function streamToBuffer(stream) {
  const chunks = [];
  for await (const chunk of stream) {
    chunks.push(chunk);
  }
  return Buffer.concat(chunks);
}

async function createVariant(buffer, width) {
  return sharp(buffer)
    .rotate()
    .resize({ width, withoutEnlargement: true })
    .webp({ quality: 80 })
    .toBuffer();
}

/* ---------------------------------------
HANDLER
--------------------------------------- */
exports.handler = async (event) => {
  console.log("📦 Event:", JSON.stringify(event));

  const db = await getDb();

  for (const record of event.Records) {
    try {
      if (!record.s3) continue;

      const bucket = record.s3.bucket.name;
      const key = decodeURIComponent(
        record.s3.object.key.replace(/\+/g, " ")
      );

      console.log("🟡 Processing:", key);

      /* ---------------------------------------
         VALIDATE PREFIX (NEW STRUCTURE)
      --------------------------------------- */
      if (!key.startsWith("uploads-temp/")) {
        console.log("⏭ Skipping non-upload key:", key);
        continue;
      }

  const parts = key.split("/");

/*
uploads-temp/designer/{userId}/{assetId}/original.jpg
*/

const entity = parts[1];
const userId = parts[2];
const assetId = parts[3];

      if (!assetId) {
        console.error("❌ Invalid key format:", key);
        continue;
      }

      console.log("🔍 Parsed:", {entity, userId, assetId });

      /* ---------------------------------------
         MARK PROCESSING
      --------------------------------------- */
      await db.collection("assets").updateOne(
        { id: assetId },
        {
          $set: {
            status: "processing",
            updatedAt: new Date().toISOString(),
          },
        }
      );

      /* ---------------------------------------
         DOWNLOAD ORIGINAL
      --------------------------------------- */
      const obj = await s3.send(
        new GetObjectCommand({ Bucket: bucket, Key: key })
      );

      const buffer = await streamToBuffer(obj.Body);
      const metadata = await sharp(buffer).metadata();

      /* ---------------------------------------
         VARIANTS
      --------------------------------------- */
      const variants = [
        { name: "thumb", width: 300 },
        { name: "feed", width: 800 },
        { name: "detail", width: 1400 },
      ];

      await Promise.all(
        variants.map(async (v) => {
          const output = await createVariant(buffer, v.width);

          const variantKey = `images/${entity}/${userId}/${assetId}/${v.name}.webp`;

          await s3.send(
            new PutObjectCommand({
              Bucket: bucket,
              Key: variantKey,
              Body: output,
              ContentType: "image/webp",
              CacheControl: "public, max-age=31536000, immutable",
            })
          );

          // ✅ UPSERT (NOT INSERT)
          await db.collection("image_variants").updateOne(
            { assetId, type: v.name },
            {
              $set: {
                key: variantKey,
                updatedAt: new Date().toISOString(),
              },
              $setOnInsert: {
                assetId,
                type: v.name,
                createdAt: new Date().toISOString(),
              },
            },
            { upsert: true }
          );
        })
      );

      /* ---------------------------------------
         FINALIZE ASSET
      --------------------------------------- */
      await db.collection("assets").updateOne(
        { id: assetId },
        {
          $set: {
            status: "ready",
            width: metadata.width,
            height: metadata.height,
            updatedAt: new Date().toISOString(),
          },
        }
      );

      console.log("🟢 Completed asset:", assetId);

    } catch (err) {
      console.error("❌ Error:", err);

      try {
        const key = record?.s3?.object?.key || "";
        const parts = key.split("/");
        const assetId = parts[3];

        if (assetId) {
          await db.collection("assets").updateOne(
            { id: assetId },
            {
              $set: {
                status: "failed",
                failureReason: err.message,
                updatedAt: new Date().toISOString(),
              },
            }
          );
        }
      } catch (e) {
        console.error("❌ Failure update error:", e);
      }

      throw err;
    }
  }
};