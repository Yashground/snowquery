import snowflake from 'snowflake-sdk';
import fs from 'fs';
import path from 'path';

let connectionPool: snowflake.Connection | null = null;

function getConnection(): Promise<snowflake.Connection> {
    return new Promise((resolve, reject) => {
        if (connectionPool && connectionPool.isUp()) {
            resolve(connectionPool);
            return;
        }

        // Key-pair auth bypasses MFA for programmatic access
        let privateKey = process.env.SNOWFLAKE_PRIVATE_KEY;

        if (!privateKey) {
            try {
                const privateKeyPath = path.join(process.cwd(), 'snowflake_rsa_key.p8');
                privateKey = fs.readFileSync(privateKeyPath, 'utf8');
            } catch (err) {
                reject(new Error("SNOWFLAKE_PRIVATE_KEY environment variable is missing and local .p8 file was not found."));
                return;
            }
        }

        // Format the key to ensure it is always perfectly formulated for OpenSSL
        if (privateKey) {
            // Strip out ANY existing headers, agnostically removing them and any whitespace
            let cleanKey = privateKey
                .replace(/-----BEGIN PRIVATE KEY-----/gi, '')
                .replace(/-----END PRIVATE KEY-----/gi, '')
                .replace(/-----BEGIN RSA PRIVATE KEY-----/gi, '')
                .replace(/-----END RSA PRIVATE KEY-----/gi, '')
                // CRITICAL: Vercel often wraps multi-line environment variables in literal double quotes `"` or single quotes `'`.
                // Plus, it might double-escape newlines.
                // We strip absolutely EVERYTHING that isn't a valid Base64 character (A-Z, a-z, 0-9, +, /, =)
                .replace(/[^A-Za-z0-9+/=]/g, '');

            // Snowflake and OpenSSL require newlines approximately every 64 characters for the raw base64 string
            const chunked = cleanKey.match(/.{1,64}/g)?.join('\n') || cleanKey;

            // Re-wrap it with perfect boundaries and real newlines
            privateKey = `-----BEGIN PRIVATE KEY-----\n${chunked}\n-----END PRIVATE KEY-----`;
        }

        const conn = snowflake.createConnection({
            account: process.env.SNOWFLAKE_ACCOUNT || '',
            username: process.env.SNOWFLAKE_USERNAME || '',
            authenticator: 'SNOWFLAKE_JWT',
            privateKey: privateKey,
            database: process.env.SNOWFLAKE_DATABASE || 'COVID19_EPIDEMIOLOGICAL_DATA',
            schema: process.env.SNOWFLAKE_SCHEMA || 'PUBLIC',
            warehouse: process.env.SNOWFLAKE_WAREHOUSE || 'SNOWQUERY_WH',
        });

        conn.connect((err) => {
            if (err) {
                console.error('Snowflake connection error:', err);
                reject(err);
            } else {
                connectionPool = conn;
                resolve(conn);
            }
        });
    });
}

export async function executeQuery(sql: string): Promise<{ columns: string[]; rows: Record<string, unknown>[] }> {
    const conn = await getConnection();

    return new Promise((resolve, reject) => {
        conn.execute({
            sqlText: sql,
            complete: (err, stmt, rows) => {
                if (err) {
                    reject(new Error(`Query execution failed: ${err.message}`));
                    return;
                }

                if (!rows || rows.length === 0) {
                    resolve({ columns: [], rows: [] });
                    return;
                }

                const columns = Object.keys(rows[0] as Record<string, unknown>);
                resolve({
                    columns,
                    rows: rows as Record<string, unknown>[],
                });
            },
        });
    });
}

export async function getTableSchema(): Promise<{ tables: { name: string; columns: { name: string; type: string }[] }[] }> {
    const sql = `
    SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE
    FROM COVID19_EPIDEMIOLOGICAL_DATA.INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = 'PUBLIC'
    ORDER BY TABLE_NAME, ORDINAL_POSITION
  `;

    const { rows } = await executeQuery(sql);

    const tableMap = new Map<string, { name: string; type: string }[]>();
    for (const row of rows) {
        const tableName = String(row.TABLE_NAME);
        const colName = String(row.COLUMN_NAME);
        const dataType = String(row.DATA_TYPE);

        if (!tableMap.has(tableName)) {
            tableMap.set(tableName, []);
        }
        tableMap.get(tableName)!.push({ name: colName, type: dataType });
    }

    return {
        tables: Array.from(tableMap.entries()).map(([name, columns]) => ({
            name,
            columns,
        })),
    };
}
