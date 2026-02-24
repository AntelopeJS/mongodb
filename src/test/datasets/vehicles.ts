export type Vehicle = {
  _id?: string;
  car: string;
  manufactured: Date;
  price: number;
  isElectric: boolean;
  kilometers: number;
};

export const Vehicle = {
  fields: {
    car: 'string',
    manufactured: 'date',
    price: 'number',
    isElectric: 'boolean',
    kilometers: 'number',
  },
  indexes: {
    isElectric: {},
  },
};

export const vehicles: Vehicle[] = [
  {
    car: 'Peugeot',
    manufactured: new Date('2003-01-01'),
    price: 3000,
    isElectric: false,
    kilometers: 9876543210,
  },
  {
    car: 'Renault',
    manufactured: new Date('1960-06-30'),
    price: -1000,
    isElectric: false,
    kilometers: 123456789012345,
  },
  {
    car: 'Citroen',
    manufactured: new Date('2040-12-31'),
    price: 0,
    isElectric: true,
    kilometers: -100000000000000,
  },
];
