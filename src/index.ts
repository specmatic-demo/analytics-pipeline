import express, { type Request, type Response } from 'express';
import mqtt, { type MqttClient } from 'mqtt';
import type {
  NotificationDeliveryAckEvent,
  NotificationStats,
  TrendInterval,
  UserNotificationEvent
} from './types';

const app = express();
const host = process.env.ANALYTICS_HOST || '0.0.0.0';
const port = Number.parseInt(process.env.ANALYTICS_PORT || '9000', 10);
const mqttUrl = process.env.ANALYTICS_MQTT_URL || 'mqtt://localhost:1883';
const userNotificationTopic = process.env.NOTIFICATION_USER_TOPIC || 'notification/user';
const notificationAckTopic = process.env.NOTIFICATION_ACK_TOPIC || 'notification/ack';
const trendIntervals = new Set(['DAY', 'WEEK', 'MONTH']);

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

function startNotificationMqttListener(): MqttClient {
  const client = mqtt.connect(mqttUrl, { reconnectPeriod: 1000 });

  client.on('connect', () => {
    console.log(`analytics-pipeline connected to MQTT broker at ${mqttUrl}`);
    client.subscribe([userNotificationTopic, notificationAckTopic], (error?: Error | null) => {
      if (error) {
        console.error(`analytics-pipeline failed to subscribe MQTT topics: ${error.message}`);
        return;
      }

      console.log(
        `analytics-pipeline subscribed to MQTT topics: ${userNotificationTopic}, ${notificationAckTopic}`
      );
    });
  });

  client.on('message', (topic: string, payload: Buffer) => {
    const parsed = parseJsonPayload(payload);
    if (parsed === null) {
      console.error(`analytics-pipeline received non-JSON MQTT payload on topic ${topic}`);
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
  });

  client.on('error', (error: Error) => {
    console.error(`analytics-pipeline MQTT error: ${error.message}`);
  });

  return client;
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

const mqttClient = startNotificationMqttListener();

app.listen(port, host, () => {
  console.log(`analytics-pipeline listening on http://${host}:${port}`);
});

function shutdown(): void {
  mqttClient.end(true, () => {
    process.exit(0);
  });
}

process.once('SIGINT', shutdown);
process.once('SIGTERM', shutdown);
