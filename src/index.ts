import express, { type Request, type Response } from 'express';
import { Kafka, type Consumer } from 'kafkajs';
import type {
  NotificationDeliveryAckEvent,
  NotificationStats,
  TrendInterval,
  UserNotificationEvent
} from './types';

const app = express();
const host = process.env.ANALYTICS_HOST || '0.0.0.0';
const port = Number.parseInt(process.env.ANALYTICS_PORT || '9000', 10);
const kafkaBrokers = (process.env.ANALYTICS_KAFKA_BROKERS || 'localhost:9092')
  .split(',')
  .map((value) => value.trim())
  .filter(Boolean);
const userNotificationTopic = process.env.NOTIFICATION_USER_TOPIC || 'notification.user';
const notificationAckTopic = process.env.NOTIFICATION_ACK_TOPIC || 'notification.ack';
const analyticsConsumerGroup = process.env.ANALYTICS_CONSUMER_GROUP || 'analytics-pipeline-group';
const trendIntervals = new Set(['DAY', 'WEEK', 'MONTH']);

const kafka = new Kafka({
  clientId: 'analytics-pipeline',
  brokers: kafkaBrokers
});
const consumer: Consumer = kafka.consumer({ groupId: analyticsConsumerGroup });

const notificationStats: NotificationStats = {
  received: 0,
  ackDelivered: 0,
  ackFailed: 0
};

function isValidDateString(value: string): boolean {
  if (typeof value !== 'string') {
    return false;
  }

  if (!/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return false;
  }

  const parsed = new Date(`${value}T00:00:00.000Z`);
  if (Number.isNaN(parsed.getTime())) {
    return false;
  }

  return parsed.toISOString().slice(0, 10) === value;
}

function readDateQueryParam(query: Request['query'], name: string): string | null {
  const value = query[name];
  if (typeof value !== 'string' || !isValidDateString(value)) {
    return null;
  }

  return value;
}

function isObjectPayload(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null;
}

function parseUserNotification(payload: unknown): UserNotificationEvent | null {
  if (!isObjectPayload(payload)) {
    return null;
  }

  const { notificationId, requestId, title, body, priority } = payload;
  const validPriority = priority === 'LOW' || priority === 'NORMAL' || priority === 'HIGH';
  if (
    typeof notificationId !== 'string' ||
    typeof requestId !== 'string' ||
    typeof title !== 'string' ||
    typeof body !== 'string' ||
    !validPriority
  ) {
    return null;
  }

  return { notificationId, requestId, title, body, priority };
}

function parseDeliveryAck(payload: unknown): NotificationDeliveryAckEvent | null {
  if (!isObjectPayload(payload)) {
    return null;
  }

  const { requestId, notificationId, acknowledgedAt, status, failureReason } = payload;
  const validStatus = status === 'DELIVERED' || status === 'FAILED';
  if (
    typeof requestId !== 'string' ||
    typeof notificationId !== 'string' ||
    typeof acknowledgedAt !== 'string' ||
    !validStatus
  ) {
    return null;
  }

  if (typeof failureReason !== 'undefined' && typeof failureReason !== 'string') {
    return null;
  }

  return { requestId, notificationId, acknowledgedAt, status, failureReason };
}

function parseJsonPayload(buffer: Buffer): unknown | null {
  try {
    return JSON.parse(buffer.toString('utf-8')) as unknown;
  } catch {
    return null;
  }
}

async function startNotificationKafkaListener(): Promise<void> {
  await consumer.connect();
  await consumer.subscribe({ topic: userNotificationTopic, fromBeginning: false });
  await consumer.subscribe({ topic: notificationAckTopic, fromBeginning: false });
  console.log(`analytics-pipeline connected to Kafka broker at ${kafkaBrokers.join(',')}`);
  console.log(
    `analytics-pipeline subscribed to Kafka topics: ${userNotificationTopic}, ${notificationAckTopic}`
  );

  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      if (!message.value) {
        return;
      }

      const parsed = parseJsonPayload(message.value);
      if (parsed === null) {
        console.error(`analytics-pipeline received non-JSON payload on topic ${topic}`);
        return;
      }

      if (topic === userNotificationTopic) {
        const event = parseUserNotification(parsed);
        if (!event) {
          console.error(`analytics-pipeline received invalid UserNotification payload on ${topic}`);
          return;
        }

        notificationStats.received += 1;
        return;
      }

      if (topic === notificationAckTopic) {
        const event = parseDeliveryAck(parsed);
        if (!event) {
          console.error(`analytics-pipeline received invalid NotificationDeliveryAck payload on ${topic}`);
          return;
        }

        if (event.status === 'DELIVERED') {
          notificationStats.ackDelivered += 1;
        } else {
          notificationStats.ackFailed += 1;
        }
      }
    }
  });
}

app.get('/analytics/orders/daily-summary', (req: Request, res: Response) => {
  const date = typeof req.query.date === 'string' ? req.query.date : '';
  if (!isValidDateString(date)) {
    res.status(400).json({ error: 'Invalid or missing date query parameter' });
    return;
  }

  res.status(200).json({
    date,
    totalOrders: 125,
    grossRevenue: 15675.5,
    avgOrderValue: 125.4
  });
});

app.get('/analytics/orders/trend', (req: Request, res: Response) => {
  const from = readDateQueryParam(req.query, 'from');
  const to = readDateQueryParam(req.query, 'to');
  const interval = typeof req.query.interval === 'string' ? req.query.interval : '';

  if (!from || !to || !trendIntervals.has(interval)) {
    res.status(400).json({ error: 'Invalid query parameters' });
    return;
  }
  const parsedInterval = interval as TrendInterval;

  if (from > to) {
    res.status(400).json({ error: 'Invalid query parameters' });
    return;
  }

  res.status(200).json({
    from,
    to,
    interval: parsedInterval,
    points: [
      {
        bucketStart: from,
        totalOrders: 120,
        grossRevenue: 15400.25
      }
    ]
  });
});

app.get('/analytics/customers/tier-distribution', (req: Request, res: Response) => {
  const date = readDateQueryParam(req.query, 'date');

  if (!date) {
    res.status(400).json({ error: 'Invalid query parameters' });
    return;
  }

  res.status(200).json({
    date,
    tiers: [
      { tier: 'STANDARD', customers: 950 },
      { tier: 'GOLD', customers: 320 },
      { tier: 'PLATINUM', customers: 80 }
    ]
  });
});

void startNotificationKafkaListener().catch((error: unknown) => {
  const message = error instanceof Error ? error.message : String(error);
  console.error(`analytics-pipeline Kafka error: ${message}`);
});

app.listen(port, host, () => {
  console.log(`analytics-pipeline listening on http://${host}:${port}`);
});
