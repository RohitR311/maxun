import IORedis from 'ioredis';

// Initialize Redis connection
const redisClient = new IORedis({
  host: process.env.REDIS_HOST || 'localhost',
  port: process.env.REDIS_PORT ? parseInt(process.env.REDIS_PORT, 10) : 6379,
  maxRetriesPerRequest: null,
  password: process.env.REDIS_PASSWORD ? process.env.REDIS_PASSWORD : undefined,
});

redisClient.on('connect', () => {
  console.log('Redis connected successfully.');
});

redisClient.on('error', (err) => {
  console.error('Error connecting to Redis:', err);
});


export { redisClient };