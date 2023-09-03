const axios = require('axios');
const fs = require('fs');
const readline = require('readline');
const fileStream = fs.createReadStream('events.jsonl');

const rl = readline.createInterface({
  input: fileStream,
  crlfDelay: Infinity,
});

const apiUrl = 'http://localhost:8000/liveEvent';

const sendEvent = async (event) => {
  try {
    const response = await axios.post(apiUrl, event, {
      headers: { Authorization: 'secret' },
    });
    console.log(response.data.message);
  } catch (error) {
    console.error(error.message);
  }
};

rl.on('line', (line) => {
  const event = JSON.parse(line);
  sendEvent(event);
});
