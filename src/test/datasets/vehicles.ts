export type Vehicle = {
  id?: string;
  car: string;
  manufactured: Date;
  price: number;
  isElectric: boolean;
  kilometers: bigint;
};

export const vehicles: Vehicle[] = [
  {
    car: 'Peugeot',
    manufactured: new Date('2003-01-01'),
    price: 3000,
    isElectric: false,
    kilometers: BigInt(9876543210),
  },
  {
    car: 'Renault',
    manufactured: new Date('1960-06-30'),
    price: -1000,
    isElectric: false,
    kilometers: BigInt(123456789012345),
  },
  {
    car: 'Citroen',
    manufactured: new Date('2040-12-31'),
    price: 0,
    isElectric: true,
    kilometers: BigInt(-100000000000000),
  },
];
