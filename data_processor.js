const fs = require('fs');
const readline = require('readline');
const { Pool } = require('pg');

const pool = new Pool({
  user: 'postgres',
  host: 'localhost',
  database: 'finonex',
  password: 'q1w2e3r4',
  port: 5432,
});

async function processData() {
  const fileStream = fs.createReadStream('server_events.jsonl');
  const rl = readline.createInterface({
    input: fileStream,
    crlfDelay: Infinity,
  });

  const userRevenues = new Map();

  for await (const line of rl) {
    const event = JSON.parse(line);
    const revenueChange = event.name === 'add_revenue' ? event.value : -event.value;
    if (!userRevenues.has(event.userId)) {
      userRevenues.set(event.userId, revenueChange);
    } else {
      userRevenues.set(event.userId, userRevenues.get(event.userId) + revenueChange);
    }
  }

  const client = await pool.connect();

  try {
      await client.query('BEGIN');
      for (const [userId, revenueChange] of userRevenues.entries()) {
        
        await client.query(
          `INSERT INTO users_revenue (user_id, revenue) VALUES ($1, $2)
           ON CONFLICT (user_id)
           DO UPDATE SET revenue = users_revenue.revenue + $2`,
          [userId, revenueChange]
        );
      }
     await client.query('COMMIT');
     console.log('Data processing completed successfully.');
  } catch (err) {
      await client.query('ROLLBACK'); 
      console.error('Error processing data:', err);
  } finally {
      client.release();
      pool.end();
  }
}

processData();
