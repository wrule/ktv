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

export default
class KTVMap {
  public constructor(private readonly dbPath: string) {
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
async function hello() {
  const map = new KTVMap('test/ktv.db');
  console.log(await map.has('jimao'));
}
