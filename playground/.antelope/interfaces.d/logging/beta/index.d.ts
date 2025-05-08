export declare namespace Logging {
    enum Level {
        ERROR = 40,
        WARN = 30,
        INFO = 20,
        DEBUG = 10,
        TRACE = 0
    }
    /**
     * Write arguments to the main log channel at the ERROR level.
     *
     * @param args Arguments
     */
    function Error(...args: any[]): void;
    /**
     * Write arguments to the main log channel at the WARN level.
     *
     * @param args Arguments
     */
    function Warn(...args: any[]): void;
    /**
     * Write arguments to the main log channel at the INFO level.
     *
     * @param args Arguments
     */
    function Info(...args: any[]): void;
    /**
     * Write arguments to the main log channel at the DEBUG level.
     *
     * @param args Arguments
     */
    function Debug(...args: any[]): void;
    /**
     * Write arguments to the main log channel at the TRACE level.
     *
     * @param args Arguments
     */
    function Trace(...args: any[]): void;
    /**
     * Write arguments to the specified log channel.
     *
     * @param levelId Log level
     * @param channel Log channel
     * @param args Arguments
     */
    function Write(levelId: number, channel: string, ...args: any[]): void;
}
export default Logging;
