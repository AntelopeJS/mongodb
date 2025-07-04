export type User = {
  id?: string;
  name: string;
  age: number;
  email?: string;
  isActive: boolean;
  department?: string;
  skills?: string[];
  createdAt?: Date;
  lastModified?: Date;
  version?: number;
  status?: string;
  salary?: number;
  score?: bigint;
  metadata?: {
    level?: number;
    tags?: string[];
    preferences?:
      | string[]
      | {
          theme?: string;
          language?: string;
        };
  };
};

export const users: User[] = [
  {
    name: 'Antoine',
    age: 25,
    email: 'antoine@example.com',
    isActive: true,
    department: 'Development',
    skills: ['JavaScript', 'TypeScript', 'React'],
    createdAt: new Date('2023-01-15'),
    lastModified: new Date('2024-01-15'),
    version: 1,
    status: 'active',
    salary: 50000,
    score: BigInt(1000000000000000),
    metadata: {
      level: 3,
      tags: ['senior', 'frontend'],
      preferences: {
        theme: 'dark',
        language: 'fr',
      },
    },
  },
  {
    name: 'Alice',
    age: 30,
    email: 'alice@example.com',
    isActive: false,
    department: 'Marketing',
    skills: ['Photoshop', 'Illustrator', 'Design'],
    createdAt: new Date('2022-06-20'),
    lastModified: new Date('2024-01-20'),
    version: 1,
    status: 'inactive',
    salary: -15000,
    score: BigInt(-999999999999999),
    metadata: {
      level: 1,
      tags: ['junior', 'design'],
      preferences: {
        theme: 'light',
        language: 'en',
      },
    },
  },
  {
    name: 'Camille',
    age: 22,
    email: 'camille@example.com',
    isActive: true,
    department: 'Development',
    skills: ['Python', 'Django', 'PostgreSQL'],
    createdAt: new Date('2024-03-10'),
    lastModified: new Date('2024-01-25'),
    version: 1,
    status: 'active',
    salary: 0,
    score: BigInt(0),
    metadata: {
      level: 2,
      tags: ['mid-level', 'backend'],
      preferences: {
        theme: 'auto',
        language: 'fr',
      },
    },
  },
  {
    name: 'Dominique',
    age: 35,
    email: 'dominique@example.com',
    isActive: true,
    department: 'Management',
    skills: ['Embedded C', 'C++'],
    createdAt: new Date('2021-12-05'),
    lastModified: new Date('2024-01-30'),
    version: 1,
    status: 'active',
    salary: 90000,
    score: BigInt('999999999999999999'),
    metadata: {
      level: 5,
      tags: ['senior', 'management'],
      preferences: {
        theme: 'dark',
        language: 'en',
      },
    },
  },
  {
    name: 'Emilie',
    age: 28,
    email: 'emilie@example.com',
    isActive: true,
    department: 'Development',
    skills: ['Java', 'Spring', 'Microservices'],
    createdAt: new Date('2023-08-30'),
    lastModified: new Date('2024-02-01'),
    version: 1,
    status: 'active',
    salary: 60000,
    score: BigInt(-500000000000000),
    metadata: {
      level: 4,
      tags: ['senior', 'backend'],
      preferences: {
        theme: 'light',
        language: 'fr',
      },
    },
  },
];

export function getUniqueUsers(): User[] {
  return users;
}
