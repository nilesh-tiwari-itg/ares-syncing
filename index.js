import express from 'express';
import dotenv from 'dotenv';
import { migrateOrdersFromSheet } from './orders-sync/orderSync.js';
dotenv.config();
import multer from "multer";
import { migrateProducts } from './productSync.js';
import { migrateCustomCollections } from './customCollectionSync.js';
import { migrateSmartCollections } from './smartCollectionSync.js';
import { migrateCustomers } from './customerSync.js';
import { migrateCompanies } from './companiesSync.js';
const upload = multer();
const app = express();
const PORT = process.env.PORT || 8080;

// app.use(
//     cors({
//         origin: "*", // Allow all origins
//         methods: ["GET", "POST", "OPTIONS"],
//         allowedHeaders: ["Content-Type", "Authorization"],
//     })
// );


app.use(
    express.json({
        limit: '50mb',
        verify: (req, res, buf) => {
            req.rawBody = buf
        },
    })
)

// Routes
app.use("/order", upload.single("file"), migrateOrdersFromSheet);
app.use("/product", upload.single("file"), migrateProducts);
app.use("/customers", upload.single("file"), migrateCustomers);
app.use("/companies", upload.single("file"), migrateCompanies);
app.use("/custom-collection", upload.single("file"), migrateCustomCollections);
app.use("/smart-collection", upload.single("file"), migrateSmartCollections);

// Start server
app.listen(PORT, () => {
    console.log(`Server is running at http://localhost:${PORT}`);
});

//for vercel
export default app;
