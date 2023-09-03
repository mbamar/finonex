const express = require('express');
const fs = require('fs');
const bodyParser = require('body-parser');
const { Pool } = require('pg');
const app = express();
const port = 8000;

app.use(bodyParser.json());


const pool = new Pool({
  user: 'postgres',
  host: 'localhost',
  database: 'finonex',
  password: 'q1w2e3r4',
  port: 5432,
});

app.post('/liveEvent', (req, res) => {
  const authorizationHeader = req.headers.authorization;
  if (authorizationHeader !== 'secret') {
    return res.status(401).json({ message: 'Unauthorized' });
  }

  const event = req.body;
  fs.appendFile('server_events.jsonl', JSON.stringify(event) + '\n', (err) => {
    if (err) {
      console.error(err);
      return res.status(500).json({ message: 'Internal Server Error' });
    }
    return res.status(200).json({ message: 'Event received and saved' });
  });
});


app.get('/userEvents/:userId', async (req, res) => {
  const userId = req.params.userId;
  try {
      const client = await pool.connect();
      const result = await client.query(
        'SELECT * FROM users_revenue WHERE user_id = $1',
        [userId]
      );
      client.release();

      if (result.rowCount === 0) {
        res.status(404).json({ message: 'User not found' });
      } else {
        res.status(200).json(result.rows);
      }
  } catch (error) {
      console.error('Error fetching user events:', error);
      res.status(500).json({ message: 'Internal server error' });
  }
});

app.listen(port, () => {
  console.log(`Server is running on http://localhost:${port}`);
});
