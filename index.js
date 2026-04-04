console.log("🚀 FILE LOADED");
const sharp = require("sharp");
const {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
} = require("@aws-sdk/client-s3");
const admin = require("firebase-admin");

/* ---------------------------------------
   CONFIG
--------------------------------------- */
const s3 = new S3Client({
  region: process.env.AWS_REGIONN,
});

/* ---------------------------------------
   INIT FIREBASE
--------------------------------------- */
if (!admin.apps.length) {
  admin.initializeApp({
    credential: admin.credential.cert({
      projectId: process.env.FIREBASE_PROJECT_ID,
      clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
      privateKey: process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, "\n"),
    }),
  });
}

const firestore = admin.firestore();

/* ---------------------------------------
   HELPERS
--------------------------------------- */

// Convert S3 stream → buffer
async function streamToBuffer(stream) {
  return Buffer.from(await stream.transformToByteArray());
}

// Generate image variant
async function createVariant(buffer, width) {
  return sharp(buffer)
    .resize({
      width,
      withoutEnlargement: true,
    })
    .webp({ quality: 80 })
    .toBuffer();
}

/* ---------------------------------------
   HANDLER (S3 TRIGGER)
--------------------------------------- */
exports.handler = async (event) => {
  console.log("📦 Event:", JSON.stringify(event));

  for (const record of event.Records) {
    try {
      // Safety check
      if (!record.s3) continue;

      const bucket = record.s3.bucket.name;

      const key = decodeURIComponent(
        record.s3.object.key.replace(/\+/g, " ")
      );

      console.log("🟡 Processing:", key);

      /* ---------------------------------------
         PREVENT INFINITE LOOP
      --------------------------------------- */
      if (key.startsWith("images/")) {
        console.log("⏭ Skipping processed file:", key);
        continue;
      }

      /* ---------------------------------------
         PARSE ASSET ID
      --------------------------------------- */
      // uploads-temp/{userId}/{assetId}/original
      const parts = key.split("/");
      const assetId = parts[2];

      if (!assetId) {
        console.error("❌ Invalid key format:", key);
        continue;
      }

      /* ---------------------------------------
         DOWNLOAD ORIGINAL
      --------------------------------------- */
      const obj = await s3.send(
        new GetObjectCommand({
          Bucket: bucket,
          Key: key,
        })
      );

      const buffer = await streamToBuffer(obj.Body);

      const metadata = await sharp(buffer).metadata();

      console.log("📐 Metadata:", metadata);

      /* ---------------------------------------
         GENERATE VARIANTS
      --------------------------------------- */
      const variants = [
        { name: "thumb", width: 300 },
        { name: "feed", width: 800 },
        { name: "detail", width: 1400 },
      ];

      for (const v of variants) {
        const output = await createVariant(buffer, v.width);

        const variantKey = `images/${assetId}/${v.name}.webp`;

        await s3.send(
          new PutObjectCommand({
            Bucket: bucket,
            Key: variantKey,
            Body: output,
            ContentType: "image/webp",
            CacheControl: "public, max-age=31536000, immutable",
          })
        );

        console.log(`✅ Uploaded ${v.name}: ${variantKey}`);
      }

      /* ---------------------------------------
         UPDATE FIRESTORE
      --------------------------------------- */
      await firestore.collection("assets").doc(assetId).set(
        {
          status: "ready",
          width: metadata.width,
          height: metadata.height,
          updatedAt: new Date().toISOString(),
        },
        { merge: true }
      );

      console.log("🟢 Completed asset:", assetId);

    } catch (err) {
      console.error("❌ Error processing image:", err);

      // Optional: mark failed in Firestore
      try {
        const parts = record.s3?.object?.key?.split("/") || [];
        const assetId = parts[2];

        if (assetId) {
          await firestore.collection("assets").doc(assetId).set(
            {
              status: "failed",
              error: err.message,
            },
            { merge: true }
          );
        }
      } catch (e) {
        console.error("❌ Failed to update failure state:", e);
      }

      throw err; // ensures retry
    }
  }
};