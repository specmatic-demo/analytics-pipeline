export type TrendInterval = 'DAY' | 'WEEK' | 'MONTH';
export type NotificationPriority = 'LOW' | 'NORMAL' | 'HIGH';
export type AckStatus = 'DELIVERED' | 'FAILED';

export type UserNotificationEvent = {
  notificationId: string;
  requestId: string;
  title: string;
  body: string;
  priority: NotificationPriority;
};

export type NotificationDeliveryAckEvent = {
  requestId: string;
  notificationId: string;
  acknowledgedAt: string;
  status: AckStatus;
  failureReason?: string;
};

export type NotificationStats = {
  received: number;
  ackDelivered: number;
  ackFailed: number;
};
