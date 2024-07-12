import crypto from 'crypto';
import SQLite3, { Database, RunResult } from 'sqlite3';

export
function get(db: Database, key: string) {
  return new Promise<string>((resolve, reject) => {
    db.get(`SELECT value FROM ktv WHERE key = ?`, [key], (error: Error, row: any) => {
      if (error) reject(error);
      else resolve(row.value);
    });
  });
}

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
async function hello() {
  const sqlite3 = SQLite3.verbose();
  const db = new sqlite3.Database('test/ktv.db');
  // db.serialize(() => {
  //   const stmt = db.prepare(`INSERT INTO ktv(key, time, value, mtime) VALUES(?, ?, ?, ?)`);
  //   for (let i = 0; i < 10; i++) {
  //     stmt.run(crypto.randomUUID(), Date.now(), (i + 1).toString(), Date.now());
  //   }
  //   stmt.finalize();

  //   db.each('SELECT * FROM ktv', (err, row: any) => {
  //     console.log(row);
  //   });
  // });
  console.log(await set(db, 'yzz', '大傻逼是我啊'));
  db.close();
}
