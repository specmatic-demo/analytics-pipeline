import express, { type Request, type Response } from 'express';

const app = express();
const host = process.env.ANALYTICS_HOST || '0.0.0.0';
const port = Number.parseInt(process.env.ANALYTICS_PORT || '9000', 10);

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

app.listen(port, host, () => {
  console.log(`analytics-pipeline listening on http://${host}:${port}`);
});
