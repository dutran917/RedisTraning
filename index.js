const mysql = require('mysql');
const redis = require('redis');

const mysqlConnection = mysql.createConnection({
  host: 'localhost',
  user: 'root',
  password: '',
  database: 'redis_exercise'
});


const redisClient = redis.createClient();


(async () => {
  await redisClient.connect()
})();

redisClient.on('connect', () => console.log('Redis Client Connected'));
redisClient.on('error', (err) => console.log('Redis Client Connection Error', err))

mysqlConnection.connect((err) => {
  if (err) throw err;
  console.log('Connected to MySQL');
});




const transferDataToRedis = () => {
  mysqlConnection.query('SELECT * FROM sb_user_viewed_tmp', async (err, results) => {
    if (err) throw err;
    for (const row of results) {
      const { imei, target, ip, user_agent, created_at } = row;
      const timestamp = new Date(created_at).getTime()
      
      redisClient.hSet(`user:${imei}`, 'ip', ip)
      redisClient.hSet(`user:${imei}`, 'user_agent', user_agent)

      await redisClient.zAdd(`product:${target}:users`, [{
        score: timestamp,
        value: `${imei}:${timestamp}`
      }]);

      await redisClient.zAdd(`user:${imei}:history`, [{
        score: timestamp,
        value: `${target}:${timestamp}`
      }]);
    }
    
    console.log('Data transferred to Redis');
  });
};

transferDataToRedis();
