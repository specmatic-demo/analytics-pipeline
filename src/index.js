'use strict';

const express = require('express');

const app = express();
const host = process.env.ANALYTICS_HOST || '0.0.0.0';
const port = Number.parseInt(process.env.ANALYTICS_PORT || '9000', 10);

app.get('/analytics/orders/daily-summary', (req, res) => {
  const date = typeof req.query.date === 'string' ? req.query.date : new Date().toISOString().slice(0, 10);
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
