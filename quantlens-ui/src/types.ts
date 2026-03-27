export interface Stock {
  symbol: string;
  price: number;
  changePercent: number;
  rvol: number;
  clusterId: number;
  high52w: number;
  lastUpdate: number;
}