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

// console.log("🚀 FILE LOADED");

// const sharp = require("sharp");
// const {
//   S3Client,
//   GetObjectCommand,
//   PutObjectCommand,
//   DeleteObjectCommand,
// } = require("@aws-sdk/client-s3");

// const { MongoClient } = require("mongodb");

// /* ---------------------------------------
//    CONFIG
// --------------------------------------- */
// const REGION = process.env.AWS_REGIONN;
// const BUCKET = process.env.S3_BUCKET;
// const MONGO_URI = process.env.MONGO_URI;

// const s3 = new S3Client({ region: REGION });

// /* ---------------------------------------
//    MONGO CONNECTION (REUSED)
// --------------------------------------- */
// const client = new MongoClient(MONGO_URI);
// let db;

// async function getDb() {
//   if (!db) {
//     await client.connect();
//     db = client.db(); // default DB from URI
//   }
//   return db;
// }

// /* ---------------------------------------
//    HELPERS
// --------------------------------------- */

// // Stream → buffer (safe)
// async function streamToBuffer(stream) {
//   const chunks = [];
//   for await (const chunk of stream) {
//     chunks.push(chunk);
//   }
//   return Buffer.concat(chunks);
// }

// // Generate variant
// async function createVariant(buffer, width) {
//   return sharp(buffer)
//     .rotate()
//     .resize({
//       width,
//       withoutEnlargement: true,
//     })
//     .webp({ quality: 80 })
//     .toBuffer();
// }

// /* ---------------------------------------
//    HANDLER
// --------------------------------------- */
// exports.handler = async (event) => {
//   console.log("📦 Event:", JSON.stringify(event));

//   const db = await getDb();

//   for (const record of event.Records) {
//     try {
//       if (!record.s3) continue;

//       const bucket = record.s3.bucket.name;
//       const key = decodeURIComponent(
//         record.s3.object.key.replace(/\+/g, " ")
//       );

//       console.log("🟡 Processing:", key);

//       /* ---------------------------------------
//          VALIDATE KEY
//       --------------------------------------- */
//       if (!key.startsWith("uploads-temp/")) {
//         console.log("⏭ Skipping non-upload key:", key);
//         continue;
//       }

//       const parts = key.split("/");
//       const assetId = parts[2];

//       if (!assetId) {
//         console.error("❌ Invalid key format:", key);
//         continue;
//       }

//       /* ---------------------------------------
//          MARK PROCESSING
//       --------------------------------------- */
//       await db.collection("assets").updateOne(
//         { id: assetId },
//         {
//           $set: {
//             status: "processing",
//             updatedAt: new Date().toISOString(),
//           },
//         }
//       );

//       /* ---------------------------------------
//          DOWNLOAD ORIGINAL
//       --------------------------------------- */
//       const obj = await s3.send(
//         new GetObjectCommand({
//           Bucket: bucket,
//           Key: key,
//         })
//       );

//       const buffer = await streamToBuffer(obj.Body);

//       const metadata = await sharp(buffer).metadata();

//       console.log("📐 Metadata:", metadata);

//       /* ---------------------------------------
//          GENERATE VARIANTS (PARALLEL)
//       --------------------------------------- */
//       const variants = [
//         { name: "thumb", width: 300 },
//         { name: "feed", width: 800 },
//         { name: "detail", width: 1400 },
//       ];

//       await Promise.all(
//         variants.map(async (v) => {
//           const output = await createVariant(buffer, v.width);

//           const variantKey = `images/${assetId}/${v.name}.webp`;

//           // Upload to S3
//           await s3.send(
//             new PutObjectCommand({
//               Bucket: bucket,
//               Key: variantKey,
//               Body: output,
//               ContentType: "image/webp",
//               CacheControl: "public, max-age=31536000, immutable",
//             })
//           );

//           console.log(`✅ Uploaded ${v.name}: ${variantKey}`);

//           // ✅ STORE VARIANT IN MONGO (CRITICAL)
//           await db.collection("image_variants").insertOne({
//             assetId,
//             type: v.name,
//             key: variantKey,
//           });
//         })
//       );

//       /* ---------------------------------------
//          UPDATE ASSET (READY)
//       --------------------------------------- */
//       await db.collection("assets").updateOne(
//         { id: assetId },
//         {
//           $set: {
//             status: "ready",
//             width: metadata.width,
//             height: metadata.height,
//             updatedAt: new Date().toISOString(),
//           },
//         }
//       );

//       console.log("🟢 Completed asset:", assetId);

//       /* ---------------------------------------
//          OPTIONAL: DELETE ORIGINAL
//       --------------------------------------- */
//       /*
//       await s3.send(
//         new DeleteObjectCommand({
//           Bucket: bucket,
//           Key: key,
//         })
//       );
//       */

//     } catch (err) {
//       console.error("❌ Error processing image:", err);

//       /* ---------------------------------------
//          FAILURE HANDLING
//       --------------------------------------- */
//       try {
//         const parts = record.s3?.object?.key?.split("/") || [];
//         const assetId = parts[2];

//         if (assetId) {
//           await db.collection("assets").updateOne(
//             { id: assetId },
//             {
//               $set: {
//                 status: "failed",
//                 error: err.message,
//                 updatedAt: new Date().toISOString(),
//               },
//             }
//           );
//         }
//       } catch (e) {
//         console.error("❌ Failed to update failure state:", e);
//       }

//       throw err;
//     }
//   }
// };

// console.log("🚀 FILE LOADED");

// const sharp = require("sharp");
// const {
//   S3Client,
//   GetObjectCommand,
//   PutObjectCommand,
// } = require("@aws-sdk/client-s3");
// const admin = require("firebase-admin");

// /* ---------------------------------------
//    CONFIG
// --------------------------------------- */
// const s3 = new S3Client({
//   region: process.env.AWS_REGIONN, // ✅ FIXED
// });

// /* ---------------------------------------
//    INIT FIREBASE (SAFE)
// --------------------------------------- */
// let firestore;

// try {
//   if (!admin.apps.length) {
//     admin.initializeApp({
//       credential: admin.credential.cert({
//         projectId: process.env.FIREBASE_PROJECT_ID,
//         clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
//         privateKey: process.env.FIREBASE_PRIVATE_KEY?.replace(/\\n/g, "\n"),
//       }),
//     });
//   }

//   firestore = admin.firestore();
// } catch (err) {
//   console.error("🔥 Firebase init failed:", err);
// }

// /* ---------------------------------------
//    HELPERS
// --------------------------------------- */

// // Convert S3 stream → buffer
// async function streamToBuffer(stream) {
//   return Buffer.from(await stream.transformToByteArray());
// }

// // Generate image variant
// async function createVariant(buffer, width) {
//   return sharp(buffer)
//     .rotate() // ✅ fixes orientation
//     .resize({
//       width,
//       withoutEnlargement: true,
//     })
//     .webp({ quality: 80 })
//     .toBuffer();
// }

// /* ---------------------------------------
//    HANDLER (S3 TRIGGER)
// --------------------------------------- */
// exports.handler = async (event) => {
//   console.log("📦 Event:", JSON.stringify(event));

//   for (const record of event.Records) {
//     try {
//       if (!record.s3) continue;

//       const bucket = record.s3.bucket.name;

//       const key = decodeURIComponent(
//         record.s3.object.key.replace(/\+/g, " ")
//       );

//       console.log("🟡 Processing:", key);

//       /* ---------------------------------------
//          PREVENT INVALID / LOOP KEYS
//       --------------------------------------- */
//       if (!key.includes("uploads-temp/")) {
//         console.log("⏭ Skipping non-upload key:", key);
//         continue;
//       }

//       if (key.startsWith("images/")) {
//         console.log("⏭ Skipping processed file:", key);
//         continue;
//       }

//       /* ---------------------------------------
//          PARSE ASSET ID
//       --------------------------------------- */
//       const parts = key.split("/");
//       const assetId = parts[2];

//       if (!assetId) {
//         console.error("❌ Invalid key format:", key);
//         continue;
//       }

//       /* ---------------------------------------
//          DOWNLOAD ORIGINAL
//       --------------------------------------- */
//       const obj = await s3.send(
//         new GetObjectCommand({
//           Bucket: bucket,
//           Key: key,
//         })
//       );

//       const buffer = await streamToBuffer(obj.Body);

//       const metadata = await sharp(buffer).metadata();

//       console.log("📐 Metadata:", metadata);

//       /* ---------------------------------------
//          GENERATE VARIANTS (PARALLEL 🚀)
//       --------------------------------------- */
//       const variants = [
//         { name: "thumb", width: 300 },
//         { name: "feed", width: 800 },
//         { name: "detail", width: 1400 },
//       ];

//       await Promise.all(
//         variants.map(async (v) => {
//           const output = await createVariant(buffer, v.width);

//           const variantKey = `images/${assetId}/${v.name}.webp`;

//           await s3.send(
//             new PutObjectCommand({
//               Bucket: bucket,
//               Key: variantKey,
//               Body: output,
//               ContentType: "image/webp",
//               CacheControl: "public, max-age=31536000, immutable",
//             })
//           );

//           console.log(`✅ Uploaded ${v.name}: ${variantKey}`);
//         })
//       );

//       /* ---------------------------------------
//          UPDATE FIRESTORE
//       --------------------------------------- */
//       if (firestore) {
//         await firestore.collection("assets").doc(assetId).set(
//           {
//             status: "ready",
//             width: metadata.width,
//             height: metadata.height,
//             updatedAt: new Date().toISOString(),
//           },
//           { merge: true }
//         );
//       }

//       console.log("🟢 Completed asset:", assetId);

//     } catch (err) {
//       console.error("❌ Error processing image:", err);

//       // Optional failure tracking
//       try {
//         const parts = record.s3?.object?.key?.split("/") || [];
//         const assetId = parts[2];

//         if (assetId && firestore) {
//           await firestore.collection("assets").doc(assetId).set(
//             {
//               status: "failed",
//               error: err.message,
//               updatedAt: new Date().toISOString(),
//             },
//             { merge: true }
//           );
//         }
//       } catch (e) {
//         console.error("❌ Failed to update failure state:", e);
//       }

//       throw err; // ensures retry
//     }
//   }
// };

// console.log("🚀 FILE LOADED");
// const sharp = require("sharp");
// const {
//   S3Client,
//   GetObjectCommand,
//   PutObjectCommand,
// } = require("@aws-sdk/client-s3");
// const admin = require("firebase-admin");

// /* ---------------------------------------
//    CONFIG
// --------------------------------------- */
// const s3 = new S3Client({
//   region: process.env.AWS_REGIONN,
// });

// /* ---------------------------------------
//    INIT FIREBASE
// --------------------------------------- */
// if (!admin.apps.length) {
//   admin.initializeApp({
//     credential: admin.credential.cert({
//       projectId: process.env.FIREBASE_PROJECT_ID,
//       clientEmail: process.env.FIREBASE_CLIENT_EMAIL,
//       privateKey: process.env.FIREBASE_PRIVATE_KEY.replace(/\\n/g, "\n"),
//     }),
//   });
// }

// const firestore = admin.firestore();

// /* ---------------------------------------
//    HELPERS
// --------------------------------------- */

// // Convert S3 stream → buffer
// async function streamToBuffer(stream) {
//   return Buffer.from(await stream.transformToByteArray());
// }

// // Generate image variant
// async function createVariant(buffer, width) {
//   return sharp(buffer)
//     .resize({
//       width,
//       withoutEnlargement: true,
//     })
//     .webp({ quality: 80 })
//     .toBuffer();
// }

// /* ---------------------------------------
//    HANDLER (S3 TRIGGER)
// --------------------------------------- */
// exports.handler = async (event) => {
//   console.log("📦 Event:", JSON.stringify(event));

//   for (const record of event.Records) {
//     try {
//       // Safety check
//       if (!record.s3) continue;

//       const bucket = record.s3.bucket.name;

//       const key = decodeURIComponent(
//         record.s3.object.key.replace(/\+/g, " ")
//       );

//       console.log("🟡 Processing:", key);

//       /* ---------------------------------------
//          PREVENT INFINITE LOOP
//       --------------------------------------- */
//       if (key.startsWith("images/")) {
//         console.log("⏭ Skipping processed file:", key);
//         continue;
//       }

//       /* ---------------------------------------
//          PARSE ASSET ID
//       --------------------------------------- */
//       // uploads-temp/{userId}/{assetId}/original
//       const parts = key.split("/");
//       const assetId = parts[2];

//       if (!assetId) {
//         console.error("❌ Invalid key format:", key);
//         continue;
//       }

//       /* ---------------------------------------
//          DOWNLOAD ORIGINAL
//       --------------------------------------- */
//       const obj = await s3.send(
//         new GetObjectCommand({
//           Bucket: bucket,
//           Key: key,
//         })
//       );

//       const buffer = await streamToBuffer(obj.Body);

//       const metadata = await sharp(buffer).metadata();

//       console.log("📐 Metadata:", metadata);

//       /* ---------------------------------------
//          GENERATE VARIANTS
//       --------------------------------------- */
//       const variants = [
//         { name: "thumb", width: 300 },
//         { name: "feed", width: 800 },
//         { name: "detail", width: 1400 },
//       ];

//       for (const v of variants) {
//         const output = await createVariant(buffer, v.width);

//         const variantKey = `images/${assetId}/${v.name}.webp`;

//         await s3.send(
//           new PutObjectCommand({
//             Bucket: bucket,
//             Key: variantKey,
//             Body: output,
//             ContentType: "image/webp",
//             CacheControl: "public, max-age=31536000, immutable",
//           })
//         );

//         console.log(`✅ Uploaded ${v.name}: ${variantKey}`);
//       }

//       /* ---------------------------------------
//          UPDATE FIRESTORE
//       --------------------------------------- */
//       await firestore.collection("assets").doc(assetId).set(
//         {
//           status: "ready",
//           width: metadata.width,
//           height: metadata.height,
//           updatedAt: new Date().toISOString(),
//         },
//         { merge: true }
//       );

//       console.log("🟢 Completed asset:", assetId);

//     } catch (err) {
//       console.error("❌ Error processing image:", err);

//       // Optional: mark failed in Firestore
//       try {
//         const parts = record.s3?.object?.key?.split("/") || [];
//         const assetId = parts[2];

//         if (assetId) {
//           await firestore.collection("assets").doc(assetId).set(
//             {
//               status: "failed",
//               error: err.message,
//             },
//             { merge: true }
//           );
//         }
//       } catch (e) {
//         console.error("❌ Failed to update failure state:", e);
//       }

//       throw err; // ensures retry
//     }
//   }
// };