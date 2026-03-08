const crypto = require('crypto');
const fs = require('fs');
const path = require('path');

try {
    const privateKeyPath = path.join(process.cwd(), 'snowflake_rsa_key.p8');
    let privateKey = fs.readFileSync(privateKeyPath, 'utf8');

    console.log("Original Key length:", privateKey.length);

    // Apply our aggressive stripper
    let cleanKey = privateKey
        .replace(/-----BEGIN PRIVATE KEY-----/gi, '')
        .replace(/-----END PRIVATE KEY-----/gi, '')
        .replace(/-----BEGIN RSA PRIVATE KEY-----/gi, '')
        .replace(/-----END RSA PRIVATE KEY-----/gi, '')
        .replace(/\\n/g, '')
        .replace(/\s+/g, '');

    const chunked = cleanKey.match(/.{1,64}/g)?.join('\n') || cleanKey;

    // Try wrapping as PKCS#8
    const p8Key = `-----BEGIN PRIVATE KEY-----\n${chunked}\n-----END PRIVATE KEY-----`;

    // Try wrapping as PKCS#1
    const p1Key = `-----BEGIN RSA PRIVATE KEY-----\n${chunked}\n-----END RSA PRIVATE KEY-----`;

    console.log("Testing PKCS#8...");
    try {
        const key1 = crypto.createPrivateKey(p8Key);
        console.log("PKCS#8 Success!");
    } catch (e) {
        console.error("PKCS#8 Failed:", e.message);
    }

    console.log("\nTesting PKCS#1...");
    try {
        const key2 = crypto.createPrivateKey(p1Key);
        console.log("PKCS#1 Success!");
    } catch (e) {
        console.error("PKCS#1 Failed:", e.message);
    }

} catch (e) {
    console.error("Script error:", e.message);
}
