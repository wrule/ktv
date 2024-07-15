import fs from 'fs';
import crypto from 'crypto';
import '@wrule/xjson';
import SQLite3, { Database, RunResult } from 'sqlite3';
const sqlite3 = SQLite3.verbose();

export
function set(db: Database, key: string, value: string) {
  return new Promise<RunResult>((resolve, reject) => {
    const time = Date.now();
    db.run(
      `INSERT OR REPLACE INTO ktv(key, time, value) VALUES(?, ?, ?)`,
      [key, time, value],
      function (error: Error) {
        if (error) reject(error);
        else resolve(this);
      },
    );
  });
}

export
function add(db: Database, key: string, value: string) {
  return new Promise<RunResult>((resolve, reject) => {
    const time = Date.now();
    db.run(
      `INSERT INTO ktv(key, time, value) VALUES(?, ?, ?)`,
      [key, time, value],
      function (error: Error) {
        if (error) reject(error);
        else resolve(this);
      },
    );
  });
}

export
function del(db: Database, key: string) {
  return new Promise<RunResult>((resolve, reject) => {
    db.run(
      `DELETE FROM ktv WHERE key = ?`,
      [key],
      function (error: Error) {
        if (error) reject(error);
        else resolve(this);
      },
    );
  });
}

export
function get(db: Database, key: string) {
  return new Promise<string | undefined>((resolve, reject) => {
    db.get(
      `SELECT value FROM ktv WHERE key = ?`,
      [key],
      function (error: Error, row: any) {
        if (error) reject(error);
        else resolve(row?.value);
      },
    );
  });
}

export
function has(db: Database, key: string) {
  return new Promise<boolean>((resolve, reject) => {
    db.get(
      `SELECT 1 FROM ktv WHERE key = ?`,
      [key],
      function (error: Error, row: any) {
        if (error) reject(error);
        else resolve(!!row);
      },
    );
  });
}

function hash(text: string) {
  const hash = crypto.createHash('sha256');
  hash.update(text);
  return hash.digest('base64');
}

export
function setJSON(db: Database, key: string, object: any) {
  const hashMap = new Map<string, string>();
  const stringMap = new Map<string, string>();
  const jsonText = JSON.xstringify(object, ((key: string, value: any) => {
    if (typeof value === 'string') {
      if (value.length >= 128) {
        if (stringMap.has(value)) return stringMap.get(value);
        else {
          const hashId = hash(value);
          stringMap.set(value, hashId);
          hashMap.set(hashId, value);
          return hashId;
        }
      }
    }
    return value;
  }) as any);
  console.log(jsonText);
  // console.log(a);
}

export default
class KTVMap {
  public constructor(private readonly dbPath: string) {
    if (!fs.existsSync(this.dbPath)) {
      fs.writeFileSync(this.dbPath, Buffer.from(DB_FILE_TEMPLATE, 'base64'));
    }
    this.db = new sqlite3.Database(this.dbPath);
  }

  public readonly db: Database;

  public set(key: string, value: string) {
    return set(this.db, key, value);
  }

  public add(key: string, value: string) {
    return add(this.db, key, value);
  }

  public delete(key: string) {
    return del(this.db, key);
  }

  public get(key: string) {
    return get(this.db, key);
  }

  public has(key: string) {
    return has(this.db, key);
  }
}

export
function queryIdByHashes(db: Database, hashes: string[]) {
  return new Promise<Map<string, number>>((resolve, reject) => {
    const hashesPlaceholder = hashes.map(() => '?').join(', ');
    const selectIdSQL = `SELECT hash, id FROM hash WHERE hash IN (${hashesPlaceholder});`;
    db.all(selectIdSQL, hashes, function (error: Error, rows: any[]) {
      if (error) reject(error);
      else resolve(new Map<string, number>(rows.map((row) => [row.hash, row.id])));
    });
  });
}

export
function queryValueByIds(db: Database, ids: number[]) {
  return new Promise<Map<number, string>>((resolve, reject) => {
    const idsPlaceholder = ids.map(() => '?').join(', ');
    const selectValueSQL = `SELECT id, value FROM hash WHERE id IN (${idsPlaceholder});`;
    db.all(selectValueSQL, ids, function (error: Error, rows: any[]) {
      if (error) reject(error);
      else resolve(new Map<number, string>(rows.map((row) => [row.id, row.value])));
    });
  });
}

function insertHash(db: Database, hashMap: Map<string, string>) {
  return new Promise<Map<string, number>>((resolve, reject) => {
    const hashs = Array.from(hashMap.keys());
    const hashsPlaceholder = `${hashs.map(() => '?').join(', ')}`;
    const selectHashSQL = `SELECT hash FROM hash WHERE hash IN (${hashsPlaceholder});`;
    db.all(
      selectHashSQL,
      hashs, function (error: Error, rows: any[]) {
        if (error) reject(error);
        else {
          const existingHashs = new Set<string>(rows.map((row) => row.hash));
          const newHashs = hashs.filter((hash) => !existingHashs.has(hash));
          const insertHashSQL = newHashs.map((hash) =>
            `INSERT INTO hash (hash, value) VALUES ('${hash}', '${hashMap.get(hash)}') ON CONFLICT (hash) DO NOTHING;`
          ).join('\n');
          db.exec(insertHashSQL, async function (error: Error | null) {
            if (error) reject(error);
            else {
              resolve(await queryIdByHashes(db, hashs));
            }
          });
        }
      },
    );
  });
}

export
async function hello() {
  const map = new KTVMap('test/ktv.db');
  console.log(await queryValueByIds(map.db, [33, 34, 9999]));
  // console.log(await queryIdByHashes(map.db, ['jimao', '12', 'df']));
  // insertHash(map.db, new Map<string, string>([['4', '1.1'], ['12', '2.2'], ['1', '999'], ['5', '5'], ['16', '991'], ['17', '991']]));
  // console.log(await map.set('jimao', '新的数据库'));
}
