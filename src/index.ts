import fs from 'fs';
import crypto from 'crypto';
import { traverse, xjson_ref } from '@wrule/xjson';
import SQLite3, { Database, RunResult, Statement } from 'sqlite3';
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
async function setJSON(db: Database, key: string, object: any) {
  const hashMap = new Map<string, string>();
  let jsonText = JSON.xstringify(object, (_, value: any) => {
    if (typeof value === 'string' && value.length >= 32) {
      const hashId = hash(value);
      hashMap.set(hashId, value);
      return hashId;
    }
    return value;
  });
  const hashIdMap = await saveHashValues(db, Array.from(hashMap.entries()));
  Array.from(hashIdMap.entries()).forEach(([hash, id]) => {
    jsonText = jsonText.replaceAll(hash, xjson_ref + id);
  });
  return await set(db, key, jsonText);
}

export
async function getJSON(db: Database, key: string) {
  const jsonText = await get(db, key);
  if (jsonText === undefined) return undefined;
  const jsonObject = JSON.parse(jsonText);
  const ids = new Set<number>();
  traverse(jsonObject, (value) => {
    if (typeof value === 'string' && value.startsWith(xjson_ref))
      ids.add(Number(value.slice(xjson_ref.length)));
    return value;
  });
  const idValueMap = await queryValueByIds(db, Array.from(ids));
  return JSON.xjson_de(jsonObject, (value: any) => {
    if (typeof value === 'string' && value.startsWith(xjson_ref)) {
      const id = Number(value.slice(xjson_ref.length));
      if (idValueMap.has(id)) return idValueMap.get(id);
    }
    return value;
  });
}

export default
class KTVMap {
  public constructor(private readonly dbPath: string) {
    if (!fs.existsSync(this.dbPath)) {
      fs.writeFileSync(this.dbPath, Buffer.from('', 'base64'));
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

export
function queryHashByHashes(db: Database, hashes: string[]) {
  return new Promise<Set<string>>((resolve, reject) => {
    const hashesPlaceholder = hashes.map(() => '?').join(', ');
    const selectHashSQL = `SELECT hash FROM hash WHERE hash IN (${hashesPlaceholder});`;
    db.all(selectHashSQL, hashes, function (error: Error, rows: any[]) {
      if (error) reject(error);
      else resolve(new Set<string>(rows.map((row) => row.hash)));
    });
  });
}

export
function tryInsertHashValues(db: Database, hashValues: [string, string][]) {
  return new Promise<Statement>((resolve, reject) => {
    const insertSQL = hashValues.map(([hash, value]) =>
      `INSERT INTO hash (hash, value) VALUES ('${hash}', '${value}') ON CONFLICT (hash) DO NOTHING;`
    ).join('\n');
    db.exec(insertSQL, function (error: Error | null) {
      if (error) reject(error);
      else resolve(this);
    });
  });
}

async function saveHashValues(db: Database, hashValues: [string, string][]) {
  const hashes = hashValues.map(([hash]) => hash);
  const hashMap = new Map<string, string>(hashValues);
  const hashesSet = await queryHashByHashes(db, hashes);
  const newHashes = hashes.filter((hash) => !hashesSet.has(hash));
  await tryInsertHashValues(db, newHashes.map((hash) => [hash, hashMap.get(hash) as string]));
  return await queryIdByHashes(db, hashes);
}

export
async function hello() {
  const map = new KTVMap('test/ktv.db');
  await setJSON(map.db, 'pk', {
    t: NaN,
    a: undefined,
    b: Array(200).fill(0).map(() => 't').join(''),
    q: Array(1003).fill(0).map(() => 'p').join(''),
  })
  console.log(await getJSON(map.db, 'pk'));
  // console.log(await saveHashValues(map.db, [
  //   ['a1', 'b'],
  //   ['a', 'b'],
  // ]));
  // console.log(await queryIdByHashes(map.db, ['jimao', '12', 'df']));
  // insertHash(map.db, new Map<string, string>([['4', '1.1'], ['12', '2.2'], ['1', '999'], ['5', '5'], ['16', '991'], ['17', '991']]));
  // console.log(await map.set('jimao', '新的数据库'));
}
