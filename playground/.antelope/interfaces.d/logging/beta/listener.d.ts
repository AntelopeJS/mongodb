import { EventProxy } from '../../core/beta';
export interface Log {
	time: number;
	channel: string;
	levelId: number;
	args: any[];
}
declare const _default: EventProxy<(log: Log) => void>;
export default _default;
