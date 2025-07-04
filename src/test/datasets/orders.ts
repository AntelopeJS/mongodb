export type OrderItem = {
  id?: string;
  name?: string;
  sku?: string;
  productSku?: string;
  productName?: string;
  price?: number;
  unitPrice?: number;
  quantity: number;
  totalPrice?: number;
  category: string;
};

export type Order = {
  id?: string;
  orderId: string;
  customerName?: string;
  customerEmail?: string;
  customerId?: string;
  customer?: {
    email: string;
    name: string;
  };
  deliveryType?: string;
  orderDate: Date;
  totalAmount: number;
  isPaid: boolean;
  status?: string;
  items?: OrderItem[];
  caddy?: OrderItem[];
  productSku?: string;
  quantity?: number;
  totalPrice?: number;
  metadata?: {
    source?: string;
    priority?: number;
  };
};

export const orders: Order[] = [
  {
    orderId: 'ORD-001',
    customerName: 'Antoine',
    customerEmail: 'antoine@example.com',
    customer: {
      email: 'antoine@example.com',
      name: 'Antoine',
    },
    deliveryType: 'express',
    orderDate: new Date('2024-01-15'),
    caddy: [
      { name: 'Laptop', price: 1200, quantity: 1, category: 'electronics' },
      { name: 'Mouse', price: 25, quantity: 2, category: 'accessories' },
    ],
    items: [
      { sku: 'LAPTOP-001', quantity: 1, category: 'electronics' },
      { sku: 'BOOK-001', quantity: 2, category: 'books' },
    ],
    totalAmount: 1250,
    totalPrice: 1250,
    isPaid: true,
    status: 'completed',
    productSku: 'LAPTOP-001',
    quantity: 1,
    metadata: {
      source: 'web',
      priority: 1,
    },
  },
  {
    orderId: 'ORD-002',
    customerName: 'Alice',
    customerEmail: 'alice@example.com',
    customer: {
      email: 'alice@example.com',
      name: 'Alice',
    },
    deliveryType: 'standard',
    orderDate: new Date('2024-01-16'),
    caddy: [
      { name: 'Book', price: 15, quantity: 3, category: 'books' },
      { name: 'Pen', price: 5, quantity: 5, category: 'office' },
    ],
    items: [{ sku: 'PHONE-001', quantity: 1, category: 'electronics' }],
    totalAmount: 70,
    totalPrice: 70,
    isPaid: false,
    status: 'pending',
    productSku: 'BOOK-001',
    quantity: 2,
    metadata: {
      source: 'mobile',
      priority: 2,
    },
  },
  {
    orderId: 'ORD-003',
    customerName: 'Camille',
    customerEmail: 'camille@example.com',
    customer: {
      email: 'camille@example.com',
      name: 'Camille',
    },
    deliveryType: 'express',
    orderDate: new Date('2024-01-17'),
    caddy: [
      { name: 'Phone', price: 800, quantity: 1, category: 'electronics' },
      { name: 'Case', price: 20, quantity: 1, category: 'accessories' },
      { name: 'Charger', price: 30, quantity: 2, category: 'accessories' },
    ],
    items: [
      { sku: 'BOOK-001', quantity: 1, category: 'books' },
      { sku: 'LAPTOP-001', quantity: 1, category: 'electronics' },
    ],
    totalAmount: 880,
    totalPrice: 880,
    isPaid: true,
    status: 'completed',
    productSku: 'PHONE-001',
    quantity: 1,
    metadata: {
      source: 'web',
      priority: 1,
    },
  },
  {
    orderId: 'ORD-004',
    customerName: 'Dominique',
    customerEmail: 'dominique@example.com',
    deliveryType: 'standard',
    orderDate: new Date('2024-01-18'),
    caddy: [
      { name: 'Tablet', price: 500, quantity: 1, category: 'electronics' },
      { name: 'Keyboard', price: 80, quantity: 1, category: 'accessories' },
    ],
    totalAmount: 580,
    totalPrice: 580,
    isPaid: true,
    productSku: 'BOOK-001',
    quantity: 1,
    metadata: {
      source: 'store',
      priority: 3,
    },
  },
  {
    orderId: 'ORD-005',
    customerName: 'Emilie',
    customerEmail: 'emilie@example.com',
    deliveryType: 'express',
    orderDate: new Date('2024-01-19'),
    caddy: [
      { name: 'Monitor', price: 300, quantity: 2, category: 'electronics' },
      { name: 'Cable', price: 10, quantity: 4, category: 'accessories' },
    ],
    totalAmount: 640,
    totalPrice: 640,
    isPaid: false,
    productSku: 'PHONE-001',
    quantity: 1,
    metadata: {
      source: 'web',
      priority: 2,
    },
  },
];

export function getUniqueOrders(): Order[] {
  return orders;
}
