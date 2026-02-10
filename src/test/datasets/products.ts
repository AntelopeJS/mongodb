export type Product = {
  id?: string;
  sku: string;
  name: string;
  description?: string;
  price: number;
  category: string;
  brand?: string;
  inStock?: boolean;
  weight?: number;
  dimensions?: {
    length?: number;
    width?: number;
    height?: number;
  };
  tags?: string[];
  metadata?: {
    manufacturer?: string;
    warranty?: string;
    rating?: number;
  };
};

export const products: Product[] = [
  {
    sku: 'LAPTOP-001',
    name: 'Asell f00',
    description: 'High-performance laptop for professionals',
    price: 1200,
    category: 'electronics',
    brand: 'Asell',
    inStock: true,
    weight: 2.5,
    dimensions: {
      length: 35,
      width: 24,
      height: 2,
    },
    tags: ['laptop', 'professional', 'high-performance'],
    metadata: {
      manufacturer: 'Asell Inc.',
      warranty: '2 years',
      rating: 4.5,
    },
  },
  {
    sku: 'BOOK-001',
    name: 'Programming Fundamentals',
    description: 'Essential guide to programming concepts',
    price: 25,
    category: 'books',
    brand: 'TechBooks',
    inStock: true,
    weight: 0.8,
    dimensions: {
      length: 20,
      width: 15,
      height: 3,
    },
    tags: ['programming', 'education', 'beginner'],
    metadata: {
      manufacturer: 'TechBooks Publishing',
      warranty: 'N/A',
      rating: 4.2,
    },
  },
  {
    sku: 'PHONE-001',
    name: 'SmartPhone Pro',
    description: 'Latest smartphone with advanced features',
    price: 800,
    category: 'electronics',
    brand: 'TechCorp',
    inStock: false,
    weight: 0.2,
    dimensions: {
      length: 15,
      width: 7,
      height: 1,
    },
    tags: ['smartphone', 'mobile', 'advanced'],
    metadata: {
      manufacturer: 'TechCorp Industries',
      warranty: '1 year',
      rating: 4.8,
    },
  },
  {
    sku: 'MOUSE-001',
    name: 'Wireless Mouse',
    description: 'Ergonomic wireless mouse for comfort',
    price: 30,
    category: 'accessories',
    brand: 'ComfortTech',
    inStock: true,
    weight: 0.1,
    dimensions: {
      length: 12,
      width: 6,
      height: 4,
    },
    tags: ['mouse', 'wireless', 'ergonomic'],
    metadata: {
      manufacturer: 'ComfortTech Solutions',
      warranty: '1 year',
      rating: 4.0,
    },
  },
  {
    sku: 'KEYBOARD-001',
    name: 'Mechanical Keyboard',
    description: 'Premium mechanical keyboard for typing enthusiasts',
    price: 150,
    category: 'accessories',
    brand: 'TypeMaster',
    inStock: true,
    weight: 1.2,
    dimensions: {
      length: 45,
      width: 15,
      height: 3,
    },
    tags: ['keyboard', 'mechanical', 'premium'],
    metadata: {
      manufacturer: 'TypeMaster Corp',
      warranty: '3 years',
      rating: 4.7,
    },
  },
];

export function getUniqueProducts(): Product[] {
  return products;
}
