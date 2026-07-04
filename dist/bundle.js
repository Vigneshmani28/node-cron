var __getOwnPropNames = Object.getOwnPropertyNames;
var __commonJS = (cb, mod) => function __require() {
  try {
    return mod || (0, cb[__getOwnPropNames(cb)[0]])((mod = { exports: {} }).exports, mod), mod.exports;
  } catch (e) {
    throw mod = 0, e;
  }
};

// node_modules/node-cron/dist/_shared.cjs
var require_shared = __commonJS({
  "node_modules/node-cron/dist/_shared.cjs"(exports2) {
    "use strict";
    var events = require("events");
    var node_crypto = require("node:crypto");
    function createID() {
      return node_crypto.randomUUID();
    }
    var levelColors = {
      INFO: "\x1B[36m",
      WARN: "\x1B[33m",
      ERROR: "\x1B[31m",
      DEBUG: "\x1B[35m"
    };
    var GREEN = "\x1B[32m";
    var RESET = "\x1B[0m";
    function log(level, message, extra) {
      const timestamp = (/* @__PURE__ */ new Date()).toISOString();
      const color = levelColors[level] ?? "";
      const prefix = `[${timestamp}] [PID: ${process.pid}] ${GREEN}[NODE-CRON]${GREEN} ${color}[${level}]${RESET}`;
      const output = `${prefix} ${message}`;
      switch (level) {
        case "ERROR":
          console.error(output, extra ?? "");
          break;
        case "DEBUG":
          console.debug(output, extra ?? "");
          break;
        case "WARN":
          console.warn(output);
          break;
        case "INFO":
        default:
          console.info(output);
          break;
      }
    }
    var defaultLogger = {
      info(message) {
        log("INFO", message);
      },
      warn(message) {
        log("WARN", message);
      },
      error(message, err) {
        if (message instanceof Error) {
          log("ERROR", message.message, message);
        } else {
          log("ERROR", message, err);
        }
      },
      debug(message, err) {
        if (message instanceof Error) {
          log("DEBUG", message.message, message);
        } else {
          log("DEBUG", message, err);
        }
      }
    };
    var noopLogger = {
      info() {
      },
      warn() {
      },
      error() {
      },
      debug() {
      }
    };
    var activeLogger = defaultLogger;
    function setLogger(logger2) {
      activeLogger = logger2 ?? defaultLogger;
    }
    var logger = {
      info: (message) => activeLogger.info(message),
      warn: (message) => activeLogger.warn(message),
      error: (message, err) => activeLogger.error(message, err),
      debug: (message, err) => activeLogger.debug(message, err)
    };
    var TrackedPromise = class {
      promise;
      error;
      state;
      value;
      constructor(executor) {
        this.state = "pending";
        this.promise = new Promise((resolve, reject) => {
          executor((value) => {
            this.state = "fulfilled";
            this.value = value;
            resolve(value);
          }, (error) => {
            this.state = "rejected";
            this.error = error;
            reject(error);
          });
        });
      }
      getPromise() {
        return this.promise;
      }
      getState() {
        return this.state;
      }
      isPending() {
        return this.state === "pending";
      }
      isFulfilled() {
        return this.state === "fulfilled";
      }
      isRejected() {
        return this.state === "rejected";
      }
      getValue() {
        return this.value;
      }
      getError() {
        return this.error;
      }
      then(onfulfilled, onrejected) {
        return this.promise.then(onfulfilled, onrejected);
      }
      catch(onrejected) {
        return this.promise.catch(onrejected);
      }
      finally(onfinally) {
        return this.promise.finally(onfinally);
      }
    };
    function planBeat(expected, now, toleranceMs, getNextMatch) {
      const missed = [];
      let slot = expected;
      while (true) {
        const nowMs = now.getTime();
        const slotMs = slot.getTime();
        if (nowMs < slotMs) {
          return { missed, next: slot };
        }
        const next = getNextMatch(slot);
        if (next.getTime() <= slotMs) {
          return { missed, next: getNextMatch(now) };
        }
        const gap = next.getTime() - slotMs;
        const lateBy = nowMs - slotMs;
        if (lateBy <= toleranceMs && lateBy < gap) {
          return { missed, run: slot, next };
        }
        missed.push(slot);
        slot = next;
      }
    }
    var DEFAULT_MISSED_EXECUTION_TOLERANCE = 1e3;
    function emptyOnFn() {
    }
    function emptySkipFn() {
    }
    function emptyHookFn() {
      return true;
    }
    var DEFAULT_COORDINATOR_TTL = 3e4;
    var Runner = class {
      timeMatcher;
      onMatch;
      noOverlap;
      maxExecutions;
      maxRandomDelay;
      missedExecutionTolerance;
      runCount;
      running;
      heartBeatTimeout;
      logger;
      onMissedExecution;
      onOverlap;
      onError;
      beforeRun;
      onFinished;
      onMaxExecutions;
      runCoordinator;
      coordinatorKeyPrefix;
      coordinatorTtl;
      onSkipped;
      constructor(timeMatcher, onMatch, options) {
        this.timeMatcher = timeMatcher;
        this.onMatch = onMatch;
        this.noOverlap = options == void 0 || options.noOverlap === void 0 ? false : options.noOverlap;
        this.maxExecutions = options?.maxExecutions;
        this.maxRandomDelay = options?.maxRandomDelay || 0;
        this.missedExecutionTolerance = options?.missedExecutionTolerance ?? DEFAULT_MISSED_EXECUTION_TOLERANCE;
        this.logger = options?.logger || logger;
        this.onMissedExecution = options?.onMissedExecution || emptyOnFn;
        this.onOverlap = options?.onOverlap || emptyOnFn;
        this.onError = options?.onError || ((date, error) => this.logger.error("Task failed with error!", error));
        this.onFinished = options?.onFinished || emptyHookFn;
        this.beforeRun = options?.beforeRun || emptyHookFn;
        this.onMaxExecutions = options?.onMaxExecutions || emptyOnFn;
        this.runCoordinator = options?.runCoordinator;
        this.coordinatorKeyPrefix = options?.coordinatorKeyPrefix || "";
        this.coordinatorTtl = options?.coordinatorTtl ?? DEFAULT_COORDINATOR_TTL;
        this.onSkipped = options?.onSkipped || emptySkipFn;
        this.runCount = 0;
        this.running = false;
      }
      onErrorFallback = (date, error) => {
        this.logger.error("Task failed with error!", error);
      };
      async runCoordinated(slot, run) {
        if (!this.runCoordinator) {
          await run();
          return;
        }
        const key = `${this.coordinatorKeyPrefix}:${slot.toISOString()}`;
        let allowed;
        try {
          allowed = await this.runCoordinator.shouldRun(key, this.coordinatorTtl);
        } catch (err) {
          this.logger.error("Run coordinator failed; skipping execution (fail-closed)", err);
          this.emitSkipped(slot, "coordinator-error");
          return;
        }
        if (!allowed) {
          this.emitSkipped(slot, "not-elected");
          return;
        }
        try {
          await run();
        } finally {
          try {
            await this.runCoordinator.onComplete?.(key);
          } catch (err) {
            this.logger.error("Run coordinator onComplete failed", err);
          }
        }
      }
      emitSkipped(slot, reason) {
        Promise.resolve(this.onSkipped(slot, reason)).catch((err) => this.onErrorFallback(slot, err));
      }
      start() {
        this.running = true;
        let lastExecution;
        let expectedNextExecution = this.timeMatcher.getNextMatch(nowWithoutMs());
        const armHeartBeat = () => {
          if (this.running) {
            clearTimeout(this.heartBeatTimeout);
            this.heartBeatTimeout = setTimeout(heartBeat, getDelay(expectedNextExecution));
          }
        };
        const runTask = (date) => {
          return new Promise(async (resolve) => {
            const execution = {
              id: createID(),
              reason: "scheduled"
            };
            const shouldExecute = await this.beforeRun(date, execution);
            const randomDelay = Math.floor(Math.random() * this.maxRandomDelay);
            if (shouldExecute) {
              const execute = async () => {
                try {
                  this.runCount++;
                  execution.startedAt = /* @__PURE__ */ new Date();
                  const result = await this.onMatch(date, execution);
                  execution.finishedAt = /* @__PURE__ */ new Date();
                  execution.result = result;
                  this.onFinished(date, execution);
                  if (this.maxExecutions && this.runCount >= this.maxExecutions) {
                    this.onMaxExecutions(date);
                    this.stop();
                  }
                } catch (error) {
                  execution.finishedAt = /* @__PURE__ */ new Date();
                  execution.error = error;
                  this.onError(date, error, execution);
                }
                resolve(true);
              };
              if (randomDelay > 0) {
                setTimeout(execute, randomDelay);
              } else {
                execute();
              }
            } else {
              resolve(true);
            }
          });
        };
        const heartBeat = async () => {
          const currentDate = nowWithoutMs();
          const plan = planBeat(expectedNextExecution, currentDate, this.missedExecutionTolerance, (date) => this.timeMatcher.getNextMatch(date));
          expectedNextExecution = plan.next;
          for (const missedSlot of plan.missed) {
            runAsync(this.onMissedExecution, missedSlot, this.onErrorFallback);
          }
          if (plan.run) {
            if (lastExecution && lastExecution.getState() === "pending") {
              runAsync(this.onOverlap, plan.run, this.onErrorFallback);
              if (this.noOverlap) {
                this.logger.warn("task still running, new execution blocked by overlap prevention!");
                armHeartBeat();
                return;
              }
            }
            const slot = plan.run;
            lastExecution = new TrackedPromise(async (resolve, reject) => {
              try {
                await this.runCoordinated(slot, () => runTask(slot));
                resolve(true);
              } catch (err) {
                reject(err);
              }
            });
          }
          armHeartBeat();
        };
        armHeartBeat();
      }
      nextRun() {
        return this.timeMatcher.getNextMatch(/* @__PURE__ */ new Date());
      }
      stop() {
        this.running = false;
        if (this.heartBeatTimeout) {
          clearTimeout(this.heartBeatTimeout);
          this.heartBeatTimeout = void 0;
        }
      }
      isStarted() {
        return !!this.heartBeatTimeout && this.running;
      }
      isStopped() {
        return !this.isStarted();
      }
      async execute() {
        const date = /* @__PURE__ */ new Date();
        const execution = {
          id: createID(),
          reason: "invoked"
        };
        try {
          const shouldExecute = await this.beforeRun(date, execution);
          if (shouldExecute) {
            this.runCount++;
            execution.startedAt = /* @__PURE__ */ new Date();
            const result = await this.onMatch(date, execution);
            execution.finishedAt = /* @__PURE__ */ new Date();
            execution.result = result;
            this.onFinished(date, execution);
          }
        } catch (error) {
          execution.finishedAt = /* @__PURE__ */ new Date();
          execution.error = error;
          this.onError(date, error, execution);
        }
      }
    };
    async function runAsync(fn, date, onError) {
      try {
        await fn(date);
      } catch (error) {
        onError(date, error);
      }
    }
    function getDelay(nextRun) {
      const maxDelay = 864e5;
      const now = /* @__PURE__ */ new Date();
      const delay = nextRun.getTime() - now.getTime();
      if (delay > maxDelay) {
        return maxDelay;
      }
      return Math.max(0, delay);
    }
    function nowWithoutMs() {
      const date = /* @__PURE__ */ new Date();
      date.setMilliseconds(0);
      return date;
    }
    var monthNamesConversion = /* @__PURE__ */ (() => {
      const months = [
        "january",
        "february",
        "march",
        "april",
        "may",
        "june",
        "july",
        "august",
        "september",
        "october",
        "november",
        "december"
      ];
      const shortMonths = [
        "jan",
        "feb",
        "mar",
        "apr",
        "may",
        "jun",
        "jul",
        "aug",
        "sep",
        "oct",
        "nov",
        "dec"
      ];
      function convertMonthName(expression, items) {
        for (let i = 0; i < items.length; i++) {
          expression = expression.replace(new RegExp(items[i], "gi"), i + 1);
        }
        return expression;
      }
      function interpret(monthExpression) {
        monthExpression = convertMonthName(monthExpression, months);
        monthExpression = convertMonthName(monthExpression, shortMonths);
        return monthExpression;
      }
      return interpret;
    })();
    var weekDayNamesConversion = /* @__PURE__ */ (() => {
      const weekDays = [
        "sunday",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday"
      ];
      const shortWeekDays = ["sun", "mon", "tue", "wed", "thu", "fri", "sat"];
      function convertWeekDayName(expression, items) {
        for (let i = 0; i < items.length; i++) {
          expression = expression.replace(new RegExp(items[i], "gi"), i);
        }
        return expression;
      }
      function convertWeekDays(expression) {
        expression = expression.replace("7", "0");
        expression = convertWeekDayName(expression, weekDays);
        return convertWeekDayName(expression, shortWeekDays);
      }
      return convertWeekDays;
    })();
    var convertAsterisksToRanges = /* @__PURE__ */ (() => {
      function convertAsterisk(expression, replecement) {
        if (expression.indexOf("*") !== -1) {
          return expression.replace("*", replecement);
        }
        return expression;
      }
      function convertAsterisksToRanges2(expressions) {
        expressions[0] = convertAsterisk(expressions[0], "0-59");
        expressions[1] = convertAsterisk(expressions[1], "0-59");
        expressions[2] = convertAsterisk(expressions[2], "0-23");
        expressions[3] = convertAsterisk(expressions[3], "1-31");
        expressions[4] = convertAsterisk(expressions[4], "1-12");
        expressions[5] = convertAsterisk(expressions[5], "0-6");
        return expressions;
      }
      return convertAsterisksToRanges2;
    })();
    var convertRanges = /* @__PURE__ */ (() => {
      function replaceWithRange(expression, text, init, end, stepTxt) {
        const step = parseInt(stepTxt);
        const numbers = [];
        let last = parseInt(end);
        let first = parseInt(init);
        if (first > last) {
          last = parseInt(init);
          first = parseInt(end);
        }
        for (let i = first; i <= last; i += step) {
          numbers.push(i);
        }
        return expression.replace(new RegExp(text, "i"), numbers.join());
      }
      function convertRange(expression) {
        const rangeRegEx = /(\d+)-(\d+)(\/(\d+)|)/;
        let match = rangeRegEx.exec(expression);
        while (match !== null && match.length > 0) {
          expression = replaceWithRange(expression, match[0], match[1], match[2], match[4] || "1");
          match = rangeRegEx.exec(expression);
        }
        return expression;
      }
      function convertAllRanges(expressions) {
        for (let i = 0; i < expressions.length; i++) {
          expressions[i] = convertRange(expressions[i]);
        }
        return expressions;
      }
      return convertAllRanges;
    })();
    var convertExpression = /* @__PURE__ */ (() => {
      function appendSecondExpression(expressions) {
        if (expressions.length === 5) {
          return ["0"].concat(expressions);
        }
        return expressions;
      }
      function removeSpaces(str) {
        return str.replace(/\s{2,}/g, " ").trim();
      }
      function normalizeIntegers(expressions) {
        for (let i = 0; i < expressions.length; i++) {
          const numbers = expressions[i].split(",");
          for (let j = 0; j < numbers.length; j++) {
            const token = String(numbers[j]).trim();
            if (/^l$/i.test(token)) {
              numbers[j] = "L";
            } else if (/^[0-7]l$/i.test(token)) {
              numbers[j] = token.toUpperCase();
            } else if (token.indexOf("#") !== -1) {
              numbers[j] = token;
            } else {
              numbers[j] = parseInt(numbers[j]);
            }
          }
          expressions[i] = numbers;
        }
        return expressions;
      }
      function interpret(expression) {
        let expressions = removeSpaces(`${expression}`).split(" ");
        expressions = appendSecondExpression(expressions);
        expressions[4] = monthNamesConversion(expressions[4]);
        expressions[5] = weekDayNamesConversion(expressions[5]);
        expressions = convertAsterisksToRanges(expressions);
        expressions = convertRanges(expressions);
        expressions = normalizeIntegers(expressions);
        return expressions;
      }
      return interpret;
    })();
    var LocalizedTime = class {
      timestamp;
      parts;
      timezone;
      constructor(date, timezone) {
        this.timestamp = date.getTime();
        this.timezone = timezone;
        this.parts = buildDateParts(date, timezone);
      }
      toDate() {
        return new Date(this.timestamp);
      }
      toISO() {
        const gmt = this.parts.gmt.replace(/^GMT/, "");
        const offset = gmt ? gmt : "Z";
        const pad = (n) => String(n).padStart(2, "0");
        return `${this.parts.year}-${pad(this.parts.month)}-${pad(this.parts.day)}T${pad(this.parts.hour)}:${pad(this.parts.minute)}:${pad(this.parts.second)}.${String(this.parts.millisecond).padStart(3, "0")}` + offset;
      }
      getParts() {
        return this.parts;
      }
    };
    function getOffsetMinutes(date, timezone) {
      const offset = parseOffsetMinutes(getTimezoneGMT(date, timezone).replace(/^GMT/, "") || "Z");
      return offset ?? 0;
    }
    function readsBackTo(timestamp, parts, timezone) {
      const p = buildDateParts(new Date(timestamp), timezone);
      return p.year === parts.year && p.month === parts.month && p.day === parts.day && p.hour === parts.hour && p.minute === parts.minute && p.second === parts.second;
    }
    function localTimeToTimestamp(parts, timezone) {
      const guess = Date.UTC(parts.year, parts.month - 1, parts.day, parts.hour, parts.minute, parts.second, parts.millisecond);
      const firstOffset = getOffsetMinutes(new Date(guess), timezone);
      const candidate1 = guess - firstOffset * 6e4;
      const secondOffset = getOffsetMinutes(new Date(candidate1), timezone);
      if (secondOffset === firstOffset) {
        return candidate1;
      }
      const candidate2 = guess - secondOffset * 6e4;
      if (readsBackTo(candidate1, parts, timezone))
        return candidate1;
      if (readsBackTo(candidate2, parts, timezone))
        return candidate2;
      return Math.max(candidate1, candidate2);
    }
    var partsFormatterCache = /* @__PURE__ */ new Map();
    var offsetFormatterCache = /* @__PURE__ */ new Map();
    function getPartsFormatter(timezone) {
      const key = timezone ?? "";
      let formatter = partsFormatterCache.get(key);
      if (!formatter) {
        const dftOptions = {
          year: "numeric",
          month: "2-digit",
          day: "2-digit",
          hour: "2-digit",
          minute: "2-digit",
          second: "2-digit",
          weekday: "short",
          hour12: false
        };
        if (timezone) {
          dftOptions.timeZone = timezone;
        }
        formatter = new Intl.DateTimeFormat("en-US", dftOptions);
        partsFormatterCache.set(key, formatter);
      }
      return formatter;
    }
    function getOffsetFormatter(timezone) {
      const key = timezone ?? "";
      let formatter = offsetFormatterCache.get(key);
      if (!formatter) {
        formatter = new Intl.DateTimeFormat("en-US", {
          timeZone: timezone,
          timeZoneName: "shortOffset"
        });
        offsetFormatterCache.set(key, formatter);
      }
      return formatter;
    }
    function buildDateParts(date, timezone) {
      const dateFormat = getPartsFormatter(timezone);
      const parts = dateFormat.formatToParts(date).filter((part) => {
        return part.type !== "literal";
      }).reduce((acc, part) => {
        acc[part.type] = part.value;
        return acc;
      }, {});
      const result = {
        day: parseInt(parts.day),
        month: parseInt(parts.month),
        year: parseInt(parts.year),
        hour: parts.hour === "24" ? 0 : parseInt(parts.hour),
        minute: parseInt(parts.minute),
        second: parseInt(parts.second),
        millisecond: date.getMilliseconds(),
        weekday: parts.weekday
      };
      let gmt;
      Object.defineProperty(result, "gmt", {
        enumerable: true,
        configurable: true,
        get() {
          return gmt ??= getTimezoneGMT(date, timezone);
        }
      });
      return result;
    }
    function parseOffsetMinutes(isoString) {
      if (isoString.endsWith("Z"))
        return 0;
      const match = isoString.match(/([+-])(\d{2}):(\d{2})$/);
      if (!match)
        return null;
      const sign = match[1] === "+" ? 1 : -1;
      return sign * (parseInt(match[2]) * 60 + parseInt(match[3]));
    }
    function getTimezoneGMT(date, timezone) {
      const fmt = getOffsetFormatter(timezone);
      const parts = fmt.formatToParts(date);
      const tzPart = parts.find((p) => p.type === "timeZoneName");
      if (!tzPart)
        return "Z";
      const tzValue = tzPart.value;
      if (tzValue === "GMT")
        return "Z";
      const match = tzValue.match(/^GMT([+-])(\d{1,2})(?::(\d{2}))?$/);
      if (!match)
        return "Z";
      const sign = match[1];
      const hoursNum = parseInt(match[2]);
      const minutesNum = parseInt(match[3] || "0");
      if (hoursNum === 0 && minutesNum === 0)
        return "Z";
      const hours = match[2].padStart(2, "0");
      const minutes = (match[3] || "00").padStart(2, "0");
      return `GMT${sign}${hours}:${minutes}`;
    }
    var LAST_DAY_TOKEN = "L";
    function lastDayOfMonth(year, month) {
      return new Date(Date.UTC(year, month, 0)).getUTCDate();
    }
    function matchesDayOfMonth(field, year, month, day) {
      if (field.includes(day))
        return true;
      if (field.includes(LAST_DAY_TOKEN) && day === lastDayOfMonth(year, month))
        return true;
      return false;
    }
    var LAST_WEEKDAY_REGEX = /^([0-7])L$/i;
    var NTH_WEEKDAY_REGEX = /^([0-7])#([1-5])$/;
    function parseLastWeekdayToken(value) {
      if (typeof value !== "string")
        return null;
      const match = LAST_WEEKDAY_REGEX.exec(value);
      if (!match)
        return null;
      const weekday = parseInt(match[1], 10);
      return weekday === 7 ? 0 : weekday;
    }
    function isLastWeekdayOfMonth(year, month, day) {
      const date = new Date(Date.UTC(year, month - 1, day));
      const inSevenDays = new Date(date.getTime());
      inSevenDays.setUTCDate(inSevenDays.getUTCDate() + 7);
      return inSevenDays.getUTCMonth() + 1 !== month;
    }
    function isNthWeekdayToken(value) {
      return typeof value === "string" && NTH_WEEKDAY_REGEX.test(value);
    }
    function parseNthWeekday(value) {
      if (typeof value !== "string")
        return null;
      const match = NTH_WEEKDAY_REGEX.exec(value);
      if (!match)
        return null;
      const weekday = parseInt(match[1], 10) % 7;
      const nth = parseInt(match[2], 10);
      return { weekday, nth };
    }
    function occurrenceInMonth(day) {
      return Math.floor((day - 1) / 7) + 1;
    }
    function matchesNthWeekday(token, year, month, day) {
      const parsed = parseNthWeekday(token);
      if (!parsed)
        return false;
      const weekday = new Date(Date.UTC(year, month - 1, day)).getUTCDay();
      if (weekday !== parsed.weekday)
        return false;
      return occurrenceInMonth(day) === parsed.nth;
    }
    function matchesDayOfWeek(field, year, month, day, weekday) {
      for (const value of field) {
        if (value === weekday)
          return true;
        if (isNthWeekdayToken(value)) {
          if (matchesNthWeekday(value, year, month, day))
            return true;
          continue;
        }
        const lastWeekday = parseLastWeekdayToken(value);
        if (lastWeekday !== null && lastWeekday === weekday && isLastWeekdayOfMonth(year, month, day)) {
          return true;
        }
      }
      return false;
    }
    var MAX_DAYS = 366 * 100;
    var MatcherWalker = class {
      baseDate;
      timeMatcher;
      timezone;
      seconds;
      minutes;
      hours;
      days;
      months;
      weekdays;
      constructor(timeMatcher, baseDate, timezone) {
        this.baseDate = baseDate;
        this.timeMatcher = timeMatcher;
        this.timezone = timezone;
        const expressions = timeMatcher.expressions;
        this.seconds = sortedAsc(expressions[0]);
        this.minutes = sortedAsc(expressions[1]);
        this.hours = sortedAsc(expressions[2]);
        this.days = expressions[3];
        this.months = expressions[4];
        this.weekdays = expressions[5];
      }
      isMatching() {
        return this.timeMatcher.match(this.baseDate);
      }
      matchNext() {
        const months = this.months;
        const days = this.days;
        const baseMs = Math.floor(this.baseDate.getTime() / 1e3) * 1e3;
        const baseParts = new LocalizedTime(new Date(baseMs), this.timezone).getParts();
        let { year, month, day } = baseParts;
        for (let i = 0; i < MAX_DAYS; i++) {
          if (months.includes(month) && matchesDayOfMonth(days, year, month, day) && this.matchesWeekday(year, month, day)) {
            const lowerBound = i === 0 ? baseParts : null;
            const found = this.firstTimeOnDay(year, month, day, lowerBound, baseMs);
            if (found !== null) {
              return new LocalizedTime(new Date(found), this.timezone);
            }
          }
          ({ year, month, day } = nextDay(year, month, day));
        }
        throw new Error("Could not find next matching date within reasonable time range");
      }
      firstTimeOnDay(year, month, day, lowerBound, baseMs) {
        const { seconds, minutes, hours } = this;
        for (const hour of hours) {
          if (lowerBound && hour < lowerBound.hour)
            continue;
          for (const minute of minutes) {
            for (const second of seconds) {
              if (lowerBound && !isLaterInDay(hour, minute, second, lowerBound))
                continue;
              const ts = localTimeToTimestamp({ year, month, day, hour, minute, second, millisecond: 0 }, this.timezone);
              if (ts > baseMs && this.timeMatcher.match(new Date(ts))) {
                return ts;
              }
            }
          }
        }
        return null;
      }
      matchesWeekday(year, month, day) {
        const weekday = new Date(Date.UTC(year, month - 1, day)).getUTCDay();
        return matchesDayOfWeek(this.weekdays, year, month, day, weekday);
      }
    };
    function nextDay(year, month, day) {
      const d = new Date(Date.UTC(year, month - 1, day + 1));
      return { year: d.getUTCFullYear(), month: d.getUTCMonth() + 1, day: d.getUTCDate() };
    }
    function sortedAsc(values) {
      return [...values].sort((a, b) => a - b);
    }
    function isLaterInDay(hour, minute, second, bound) {
      return hour * 3600 + minute * 60 + second > bound.hour * 3600 + bound.minute * 60 + bound.second;
    }
    function matchValue(allowedValues, value) {
      return allowedValues.indexOf(value) !== -1;
    }
    var TimeMatcher = class {
      timezone;
      pattern;
      expressions;
      constructor(pattern, timezone) {
        this.timezone = timezone;
        this.pattern = pattern;
        this.expressions = convertExpression(pattern);
      }
      match(date) {
        const localizedTime = new LocalizedTime(date, this.timezone);
        const parts = localizedTime.getParts();
        const runOnSecond = matchValue(this.expressions[0], parts.second);
        const runOnMinute = matchValue(this.expressions[1], parts.minute);
        const runOnHour = matchValue(this.expressions[2], parts.hour);
        const runOnDay = matchesDayOfMonth(this.expressions[3], parts.year, parts.month, parts.day);
        const runOnMonth = matchValue(this.expressions[4], parts.month);
        const weekday = parseInt(weekDayNamesConversion(parts.weekday));
        const runOnWeekDay = matchesDayOfWeek(this.expressions[5], parts.year, parts.month, parts.day, weekday);
        return runOnSecond && runOnMinute && runOnHour && runOnDay && runOnMonth && runOnWeekDay;
      }
      getNextMatch(date) {
        const walker = new MatcherWalker(this, date, this.timezone);
        const next = walker.matchNext();
        return next.toDate();
      }
    };
    var allowedTransitions = {
      "stopped": ["stopped", "idle", "destroyed"],
      "idle": ["idle", "running", "stopped", "destroyed"],
      "running": ["running", "idle", "stopped", "destroyed"],
      "destroyed": ["destroyed"]
    };
    var StateMachine = class {
      state;
      constructor(initial = "stopped") {
        this.state = initial;
      }
      changeState(state) {
        if (allowedTransitions[this.state].includes(state)) {
          this.state = state;
        } else {
          throw new Error(`invalid transition from ${this.state} to ${state}`);
        }
      }
    };
    var EnvVarRunCoordinator = class {
      envName;
      constructor(envName = "NODE_CRON_RUN") {
        this.envName = envName;
        this.read();
      }
      shouldRun() {
        return this.read();
      }
      read() {
        const value = process.env[this.envName];
        if (value !== "true" && value !== "false") {
          throw new Error(`node-cron: a \`distributed\` task needs ${this.envName} set to 'true' or 'false'. Set it to 'true' on exactly one instance and 'false' on the others, or provide a coordinator via cron.setRunCoordinator(...).`);
        }
        return value === "true";
      }
    };
    var globalRunCoordinator;
    function setRunCoordinator(coordinator) {
      globalRunCoordinator = coordinator;
    }
    function resolveRunCoordinator(perTask) {
      return perTask ?? globalRunCoordinator ?? new EnvVarRunCoordinator();
    }
    var TaskEmitter = class extends events.EventEmitter {
    };
    var InlineScheduledTask = class {
      emitter;
      cronExpression;
      timeMatcher;
      runner;
      id;
      name;
      stateMachine;
      timezone;
      logger;
      suppressMissedWarning;
      _lastRun = null;
      constructor(cronExpression, taskFn, options) {
        this.emitter = new TaskEmitter();
        this.cronExpression = cronExpression;
        this.id = createID();
        this.name = options?.name || this.id;
        this.timezone = options?.timezone;
        this.logger = options?.logger || logger;
        this.suppressMissedWarning = options?.suppressMissedWarning || false;
        this.timeMatcher = new TimeMatcher(cronExpression, options?.timezone);
        this.stateMachine = new StateMachine();
        const runnerOptions = {
          timezone: options?.timezone,
          noOverlap: options?.noOverlap,
          maxExecutions: options?.maxExecutions,
          maxRandomDelay: options?.maxRandomDelay,
          missedExecutionTolerance: options?.missedExecutionTolerance,
          logger: this.logger,
          beforeRun: (date, execution) => {
            if (execution.reason === "scheduled") {
              this.changeState("running");
            }
            this.emitter.emit("execution:started", this.createContext(date, execution));
            return true;
          },
          onFinished: (date, execution) => {
            if (execution.reason === "scheduled") {
              this.changeState("idle");
            }
            this.recordLastRun(execution);
            this.emitter.emit("execution:finished", this.createContext(date, execution));
            return true;
          },
          onError: (date, error, execution) => {
            this.logger.error(error);
            this.recordLastRun(execution);
            this.emitter.emit("execution:failed", this.createContext(date, execution));
            this.changeState("idle");
          },
          onOverlap: (date) => {
            this.emitter.emit("execution:overlap", this.createContext(date));
          },
          onMissedExecution: (date) => {
            const handled = this.emitter.listenerCount("execution:missed") > 0;
            if (!this.suppressMissedWarning && !handled) {
              this.logger.warn(`missed execution at ${date}! Possible blocking IO or high CPU user at the same process used by node-cron.`);
            }
            this.emitter.emit("execution:missed", this.createContext(date));
          },
          onMaxExecutions: (date) => {
            this.emitter.emit("execution:maxReached", this.createContext(date));
            this.destroy();
          },
          runCoordinator: options?.distributed ? resolveRunCoordinator(options?.runCoordinator) : void 0,
          coordinatorKeyPrefix: this.name,
          coordinatorTtl: options?.distributedLease,
          onSkipped: (date, reason) => {
            this.emitter.emit("execution:skipped", this.createContext(date, void 0, reason));
          }
        };
        this.runner = new Runner(this.timeMatcher, (date, execution) => {
          return taskFn(this.createContext(date, execution));
        }, runnerOptions);
      }
      getNextRun() {
        if (this.stateMachine.state !== "stopped") {
          return this.runner.nextRun();
        }
        return null;
      }
      getNextRuns(count) {
        const runs = [];
        let from = /* @__PURE__ */ new Date();
        for (let i = 0; i < count; i++) {
          from = this.timeMatcher.getNextMatch(from);
          runs.push(from);
        }
        return runs;
      }
      match(date) {
        return this.timeMatcher.match(date);
      }
      msToNext() {
        const next = this.getNextRun();
        return next ? next.getTime() - Date.now() : null;
      }
      isBusy() {
        return this.getStatus() === "running";
      }
      runsLeft() {
        if (this.runner.maxExecutions == null)
          return void 0;
        return Math.max(0, this.runner.maxExecutions - this.runner.runCount);
      }
      getPattern() {
        return this.cronExpression;
      }
      lastRun() {
        return this._lastRun;
      }
      recordLastRun(execution) {
        const date = execution.finishedAt;
        const lastRun = { date };
        if (execution.error) {
          lastRun.error = execution.error;
        } else {
          lastRun.result = execution.result;
        }
        this._lastRun = lastRun;
      }
      changeState(state) {
        if (this.runner.isStarted()) {
          this.stateMachine.changeState(state);
        }
      }
      start() {
        if (this.runner.isStopped()) {
          this.runner.start();
          this.stateMachine.changeState("idle");
          this.emitter.emit("task:started", this.createContext(/* @__PURE__ */ new Date()));
        }
      }
      stop() {
        if (this.runner.isStarted()) {
          this.runner.stop();
          this.stateMachine.changeState("stopped");
          this.emitter.emit("task:stopped", this.createContext(/* @__PURE__ */ new Date()));
        }
      }
      getStatus() {
        return this.stateMachine.state;
      }
      destroy() {
        if (this.stateMachine.state === "destroyed")
          return;
        this.stop();
        this.stateMachine.changeState("destroyed");
        this.emitter.emit("task:destroyed", this.createContext(/* @__PURE__ */ new Date()));
      }
      execute() {
        return new Promise((resolve, reject) => {
          const onFail = (context) => {
            this.off("execution:finished", onFinished);
            reject(context.execution?.error);
          };
          const onFinished = (context) => {
            this.off("execution:failed", onFail);
            resolve(context.execution?.result);
          };
          this.once("execution:finished", onFinished);
          this.once("execution:failed", onFail);
          this.runner.execute();
        });
      }
      on(event, fun) {
        this.emitter.on(event, fun);
      }
      off(event, fun) {
        this.emitter.off(event, fun);
      }
      once(event, fun) {
        this.emitter.once(event, fun);
      }
      createContext(executionDate, execution, reason) {
        const localTime = new LocalizedTime(executionDate, this.timezone);
        const ctx = {
          date: localTime.toDate(),
          dateLocalIso: localTime.toISO(),
          triggeredAt: /* @__PURE__ */ new Date(),
          task: this,
          execution
        };
        if (reason)
          ctx.reason = reason;
        return ctx;
      }
    };
    exports2.InlineScheduledTask = InlineScheduledTask;
    exports2.LocalizedTime = LocalizedTime;
    exports2.StateMachine = StateMachine;
    exports2.TimeMatcher = TimeMatcher;
    exports2.convertExpression = convertExpression;
    exports2.createID = createID;
    exports2.isNthWeekdayToken = isNthWeekdayToken;
    exports2.logger = logger;
    exports2.noopLogger = noopLogger;
    exports2.resolveRunCoordinator = resolveRunCoordinator;
    exports2.setLogger = setLogger;
    exports2.setRunCoordinator = setRunCoordinator;
  }
});

// node_modules/node-cron/dist/node-cron.cjs
var require_node_cron = __commonJS({
  "node_modules/node-cron/dist/node-cron.cjs"(exports2) {
    "use strict";
    Object.defineProperty(exports2, "__esModule", { value: true });
    var inlineScheduledTask = require_shared();
    var path = require("path");
    var url = require("url");
    var child_process = require("child_process");
    var events = require("events");
    require("node:crypto");
    var tasks = /* @__PURE__ */ new Map();
    var TaskRegistry = class {
      add(task) {
        if (this.has(task.id)) {
          throw Error(`task ${task.id} already registered!`);
        }
        tasks.set(task.id, task);
        task.on("task:destroyed", () => {
          this.remove(task);
        });
      }
      get(taskId) {
        return tasks.get(taskId);
      }
      remove(task) {
        if (this.has(task.id)) {
          task?.destroy();
          tasks.delete(task.id);
        }
      }
      all() {
        return tasks;
      }
      has(taskId) {
        return tasks.has(taskId);
      }
      killAll() {
        tasks.forEach((id) => this.remove(id));
      }
    };
    var validationRegex = /^(?:\d+|\*|\*\/\d+)$/;
    var ALLOWED_CHARS_REGEX = /^[a-zA-Z0-9-*/,# ]+$/;
    function isValidExpression(expression, min, max) {
      const options = expression;
      for (const option of options) {
        const optionAsInt = parseInt(option, 10);
        if (!Number.isNaN(optionAsInt) && (optionAsInt < min || optionAsInt > max) || !validationRegex.test(option))
          return false;
      }
      return true;
    }
    function isInvalidSecond(expression) {
      return !isValidExpression(expression, 0, 59);
    }
    function isInvalidMinute(expression) {
      return !isValidExpression(expression, 0, 59);
    }
    function isInvalidHour(expression) {
      return !isValidExpression(expression, 0, 23);
    }
    function isInvalidDayOfMonth(expression) {
      const days = expression.filter((value) => value !== "L");
      return !isValidExpression(days, 1, 31);
    }
    function isInvalidMonth(expression) {
      return !isValidExpression(expression, 1, 12);
    }
    function isInvalidWeekDay(expression) {
      const days = expression.filter((value) => !inlineScheduledTask.isNthWeekdayToken(value) && !/^[0-7]L$/.test(value));
      return !isValidExpression(days, 0, 7);
    }
    function validateFields(patterns, executablePatterns) {
      if (isInvalidSecond(executablePatterns[0]))
        throw new Error(`${patterns[0]} is a invalid expression for second`);
      if (isInvalidMinute(executablePatterns[1]))
        throw new Error(`${patterns[1]} is a invalid expression for minute`);
      if (isInvalidHour(executablePatterns[2]))
        throw new Error(`${patterns[2]} is a invalid expression for hour`);
      if (isInvalidDayOfMonth(executablePatterns[3]))
        throw new Error(`${patterns[3]} is a invalid expression for day of month`);
      if (isInvalidMonth(executablePatterns[4]))
        throw new Error(`${patterns[4]} is a invalid expression for month`);
      if (isInvalidWeekDay(executablePatterns[5]))
        throw new Error(`${patterns[5]} is a invalid expression for week day`);
    }
    var FIELDS = [
      { key: "second", label: "second", invalid: isInvalidSecond },
      { key: "minute", label: "minute", invalid: isInvalidMinute },
      { key: "hour", label: "hour", invalid: isInvalidHour },
      { key: "dayOfMonth", label: "day of month", invalid: isInvalidDayOfMonth },
      { key: "month", label: "month", invalid: isInvalidMonth },
      { key: "dayOfWeek", label: "week day", invalid: isInvalidWeekDay }
    ];
    function validateDetailed$1(pattern) {
      if (typeof pattern !== "string")
        return { valid: false, errors: [{ field: "expression", message: "pattern must be a string" }] };
      if (!ALLOWED_CHARS_REGEX.test(pattern))
        return { valid: false, errors: [{ field: "expression", value: pattern, message: "pattern includes illegal characters" }] };
      const raw = pattern.replace(/\s{2,}/g, " ").trim().split(" ");
      if (raw.length !== 5 && raw.length !== 6)
        return { valid: false, errors: [{ field: "expression", value: pattern, message: `expected 5 or 6 fields but got ${raw.length}` }] };
      const patterns = raw.length === 5 ? ["0", ...raw] : raw;
      const executable = inlineScheduledTask.convertExpression(pattern);
      const errors = [];
      FIELDS.forEach((f, i) => {
        if (f.invalid(executable[i]))
          errors.push({ field: f.key, value: patterns[i], message: `${patterns[i]} is a invalid expression for ${f.label}` });
      });
      if (errors.length)
        return { valid: false, errors };
      return {
        valid: true,
        errors: [],
        fields: {
          second: executable[0],
          minute: executable[1],
          hour: executable[2],
          dayOfMonth: executable[3],
          month: executable[4],
          dayOfWeek: executable[5]
        }
      };
    }
    function parse$1(pattern) {
      const result = validateDetailed$1(pattern);
      if (!result.valid)
        throw new Error(result.errors[0].message);
      return result.fields;
    }
    function validate$1(pattern) {
      if (typeof pattern !== "string")
        throw new TypeError("pattern must be a string!");
      if (!ALLOWED_CHARS_REGEX.test(pattern))
        throw new TypeError("pattern includes illegal characters!");
      const patterns = pattern.split(" ");
      const executablePatterns = inlineScheduledTask.convertExpression(pattern);
      if (patterns.length === 5)
        patterns.unshift("0");
      validateFields(patterns, executablePatterns);
    }
    var daemonPath = path.resolve(path.dirname(__filename), "daemon.cjs");
    var TaskEmitter = class extends events.EventEmitter {
    };
    var BackgroundScheduledTask = class {
      emitter;
      id;
      name;
      cronExpression;
      taskPath;
      options;
      forkProcess;
      stateMachine;
      logger;
      suppressMissedWarning;
      timeMatcher;
      runCount;
      runCoordinator;
      _lastRun = null;
      constructor(cronExpression, taskPath, options) {
        this.cronExpression = cronExpression;
        this.taskPath = taskPath;
        this.options = options;
        this.id = inlineScheduledTask.createID();
        this.name = options?.name || this.id;
        this.emitter = new TaskEmitter();
        this.stateMachine = new inlineScheduledTask.StateMachine("stopped");
        this.timeMatcher = new inlineScheduledTask.TimeMatcher(cronExpression, options?.timezone);
        this.runCount = 0;
        this.on("execution:started", () => {
          this.runCount++;
        });
        this.on("execution:finished", (context) => {
          this.recordLastRun(context.execution);
        });
        this.on("execution:failed", (context) => {
          this.recordLastRun(context.execution);
        });
        this.logger = options?.logger || inlineScheduledTask.logger;
        this.suppressMissedWarning = options?.suppressMissedWarning || false;
        this.runCoordinator = options?.distributed ? inlineScheduledTask.resolveRunCoordinator(options?.runCoordinator) : void 0;
        this.on("task:stopped", () => {
          this.forkProcess?.kill();
          this.forkProcess = void 0;
          this.stateMachine.changeState("stopped");
        });
        this.on("task:destroyed", () => {
          this.forkProcess?.kill();
          this.forkProcess = void 0;
          this.stateMachine.changeState("destroyed");
        });
      }
      getNextRun() {
        if (this.stateMachine.state !== "stopped") {
          return this.timeMatcher.getNextMatch(/* @__PURE__ */ new Date());
        }
        return null;
      }
      getNextRuns(count) {
        const runs = [];
        let from = /* @__PURE__ */ new Date();
        for (let i = 0; i < count; i++) {
          from = this.timeMatcher.getNextMatch(from);
          runs.push(from);
        }
        return runs;
      }
      match(date) {
        return this.timeMatcher.match(date);
      }
      msToNext() {
        const next = this.getNextRun();
        return next ? next.getTime() - Date.now() : null;
      }
      isBusy() {
        return this.getStatus() === "running";
      }
      runsLeft() {
        if (this.options?.maxExecutions == null)
          return void 0;
        return Math.max(0, this.options.maxExecutions - this.runCount);
      }
      getPattern() {
        return this.cronExpression;
      }
      lastRun() {
        return this._lastRun;
      }
      recordLastRun(execution) {
        if (!execution)
          return;
        const raw = execution.finishedAt ?? execution.startedAt;
        const date = raw ? new Date(raw) : /* @__PURE__ */ new Date();
        const lastRun = { date };
        if (execution.error) {
          lastRun.error = execution.error;
        } else {
          lastRun.result = execution.result;
        }
        this._lastRun = lastRun;
      }
      start() {
        return new Promise((resolve, reject) => {
          if (this.forkProcess) {
            return resolve(void 0);
          }
          const startTimeout = this.options?.startTimeout ?? 5e3;
          const failStart = (error) => {
            clearTimeout(timeout);
            this.forkProcess?.kill();
            this.forkProcess = void 0;
            reject(error);
          };
          const timeout = setTimeout(() => {
            failStart(new Error(`Start operation timed out after ${startTimeout}ms. The background task file may have failed to load or taken too long to import; verify it runs on its own and consider increasing the \`startTimeout\` option.`));
          }, startTimeout);
          try {
            this.forkProcess = child_process.fork(daemonPath);
            this.forkProcess.on("error", (err) => {
              failStart(new Error(`Error on daemon: ${err.message}`));
            });
            this.forkProcess.on("exit", (code, signal) => {
              if (code !== 0 && signal !== "SIGTERM") {
                const erro = new Error(`node-cron daemon exited with code ${code || signal}`);
                this.logger.error(erro);
                failStart(erro);
              }
            });
            this.forkProcess.on("message", (message) => {
              if (message.type === "coordinator:shouldRun") {
                void this.handleShouldRun(message);
                return;
              }
              if (message.type === "coordinator:complete") {
                this.runCoordinator?.onComplete?.(message.key)?.catch?.((err) => this.logger.error("Run coordinator onComplete failed", err));
                return;
              }
              if (message.event === "daemon:error") {
                failStart(message.jsonError ? deserializeError(message.jsonError) : new Error("Background task failed to start"));
                return;
              }
              if (message.jsonError) {
                if (message.context?.execution) {
                  message.context.execution.error = deserializeError(message.jsonError);
                  delete message.jsonError;
                }
              }
              if (message.context?.task?.state) {
                this.stateMachine.changeState(message.context?.task?.state);
              }
              if (message.context) {
                const execution = message.context?.execution;
                delete execution?.hasError;
                const context = this.createContext(new Date(message.context.date), execution, message.context.reason);
                this.logEvent(message.event, context);
                this.emitter.emit(message.event, context);
              }
            });
            this.once("task:started", () => {
              this.stateMachine.changeState("idle");
              clearTimeout(timeout);
              resolve(void 0);
            });
            this.forkProcess.send({
              command: "task:start",
              path: this.taskPath,
              cron: this.cronExpression,
              options: serializableOptions(this.options)
            });
          } catch (error) {
            failStart(error);
          }
        });
      }
      stop() {
        return new Promise((resolve, reject) => {
          if (!this.forkProcess) {
            return resolve(void 0);
          }
          const timeoutId = setTimeout(() => {
            clearTimeout(timeoutId);
            reject(new Error("Stop operation timed out"));
          }, 5e3);
          const cleanupAndResolve = () => {
            clearTimeout(timeoutId);
            this.off("task:stopped", onStopped);
            this.forkProcess = void 0;
            resolve(void 0);
          };
          const onStopped = () => {
            cleanupAndResolve();
          };
          this.once("task:stopped", onStopped);
          this.forkProcess.send({
            command: "task:stop"
          });
        });
      }
      getStatus() {
        return this.stateMachine.state;
      }
      destroy() {
        return new Promise((resolve, reject) => {
          if (!this.forkProcess) {
            return resolve(void 0);
          }
          const timeoutId = setTimeout(() => {
            clearTimeout(timeoutId);
            reject(new Error("Destroy operation timed out"));
          }, 5e3);
          const onDestroy = () => {
            clearTimeout(timeoutId);
            this.off("task:destroyed", onDestroy);
            resolve(void 0);
          };
          this.once("task:destroyed", onDestroy);
          this.forkProcess.send({
            command: "task:destroy"
          });
        });
      }
      execute() {
        return new Promise((resolve, reject) => {
          if (!this.forkProcess) {
            return reject(new Error("Cannot execute background task because it hasn't been started yet. Please initialize the task using the start() method before attempting to execute it."));
          }
          let timeoutId;
          if (typeof this.options?.executeTimeout === "number") {
            timeoutId = setTimeout(() => {
              cleanupListeners();
              reject(new Error("Execution timeout exceeded"));
            }, this.options.executeTimeout);
          }
          const cleanupListeners = () => {
            if (timeoutId)
              clearTimeout(timeoutId);
            this.off("execution:finished", onFinished);
            this.off("execution:failed", onFail);
          };
          const onFinished = (context) => {
            cleanupListeners();
            resolve(context.execution?.result);
          };
          const onFail = (context) => {
            cleanupListeners();
            reject(context.execution?.error || new Error("Execution failed without specific error"));
          };
          this.once("execution:finished", onFinished);
          this.once("execution:failed", onFail);
          this.forkProcess.send({
            command: "task:execute"
          });
        });
      }
      async handleShouldRun(message) {
        let allowed = false;
        let error;
        try {
          allowed = this.runCoordinator ? await this.runCoordinator.shouldRun(message.key, message.ttlMs) : false;
        } catch (err) {
          error = err?.message ?? String(err);
        }
        this.forkProcess?.send({ type: "coordinator:result", reqId: message.reqId, allowed, error });
      }
      on(event, fun) {
        this.emitter.on(event, fun);
      }
      off(event, fun) {
        this.emitter.off(event, fun);
      }
      once(event, fun) {
        this.emitter.once(event, fun);
      }
      logEvent(event, context) {
        switch (event) {
          case "execution:missed": {
            const handled = this.emitter.listenerCount("execution:missed") > 0;
            if (!this.suppressMissedWarning && !handled) {
              this.logger.warn(`missed execution at ${context.date}! Possible blocking IO or high CPU user at the same process used by node-cron.`);
            }
            break;
          }
          case "execution:overlap":
            if (this.options?.noOverlap) {
              this.logger.warn("task still running, new execution blocked by overlap prevention!");
            }
            break;
          case "execution:failed":
            if (context.execution?.error) {
              this.logger.error(context.execution.error);
            }
            break;
        }
      }
      createContext(executionDate, execution, reason) {
        const localTime = new inlineScheduledTask.LocalizedTime(executionDate, this.options?.timezone);
        const ctx = {
          date: localTime.toDate(),
          dateLocalIso: localTime.toISO(),
          triggeredAt: /* @__PURE__ */ new Date(),
          task: this,
          execution
        };
        if (reason)
          ctx.reason = reason;
        return ctx;
      }
    };
    function serializableOptions(options) {
      if (!options)
        return options;
      const { logger: _logger, runCoordinator: _runCoordinator, ...rest } = options;
      return rest;
    }
    function deserializeError(str) {
      const data = JSON.parse(str);
      const Err = globalThis[data.name] || Error;
      const err = new Err(data.message);
      if (data.stack) {
        err.stack = data.stack;
      }
      Object.keys(data).forEach((key) => {
        if (!["name", "message", "stack"].includes(key)) {
          err[key] = data[key];
        }
      });
      return err;
    }
    var moduleFilename = __filename;
    var registry = new TaskRegistry();
    function schedule(expression, func, options) {
      const task = createTask(expression, func, options);
      const started = task.start();
      if (started && typeof started.catch === "function") {
        started.catch((error) => {
          (options?.logger || inlineScheduledTask.logger).error(`Failed to start scheduled task: ${error?.message ?? error}`);
        });
      }
      return task;
    }
    function createTask(expression, func, options) {
      if (options?.distributed && !options.name) {
        throw new Error("`distributed` requires a `name` (it forms the coordination key shared across instances).");
      }
      let task;
      if (func instanceof Function) {
        task = new inlineScheduledTask.InlineScheduledTask(expression, func, options);
      } else {
        const taskPath = solvePath(func);
        task = new BackgroundScheduledTask(expression, taskPath, options);
      }
      registry.add(task);
      return task;
    }
    function solvePath(filePath) {
      if (path.isAbsolute(filePath))
        return url.pathToFileURL(filePath).href;
      if (filePath.startsWith("file://"))
        return filePath;
      const stackLines = new Error().stack?.split("\n");
      if (stackLines) {
        stackLines?.shift();
        const callerLine = stackLines?.find((line) => {
          return line.indexOf(moduleFilename) === -1;
        });
        const match = callerLine?.match(/(file:\/\/)?(((\/?)(\w:))?([/\\].+)):\d+:\d+/);
        if (match) {
          const dir = `${match[5] ?? ""}${path.dirname(match[6])}`;
          return url.pathToFileURL(path.resolve(dir, filePath)).href;
        }
      }
      throw new Error(`Could not locate task file ${filePath}`);
    }
    function validate(expression) {
      try {
        validate$1(expression);
        return true;
      } catch (e) {
        return false;
      }
    }
    var validateDetailed = validateDetailed$1;
    var parse = parse$1;
    var getTasks = registry.all;
    var getTask = registry.get;
    var nodeCron = {
      schedule,
      createTask,
      validate,
      validateDetailed,
      parse,
      getTasks,
      getTask,
      setLogger: inlineScheduledTask.setLogger,
      setRunCoordinator: inlineScheduledTask.setRunCoordinator
    };
    exports2.setLogger = inlineScheduledTask.setLogger;
    exports2.setRunCoordinator = inlineScheduledTask.setRunCoordinator;
    exports2.createTask = createTask;
    exports2.default = nodeCron;
    exports2.getTask = getTask;
    exports2.getTasks = getTasks;
    exports2.nodeCron = nodeCron;
    exports2.parse = parse;
    exports2.schedule = schedule;
    exports2.solvePath = solvePath;
    exports2.validate = validate;
    exports2.validateDetailed = validateDetailed;
  }
});

// index.js
var cron = require_node_cron();
console.log("Application started...");
cron.schedule("*/5 * * * * *", () => {
  console.log("Running every 5 seconds:", (/* @__PURE__ */ new Date()).toLocaleString());
});
process.stdin.resume();
