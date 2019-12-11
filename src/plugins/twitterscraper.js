const os = require("os");
const {get} = require("lodash/fp");
const cheerio = require("cheerio");
const fetch = require("node-fetch");
const { URL } = require("url");
const dashp = require("dashp");
const { envelope: env } = require("@sugarcube/core");
const { runCmd, counter } = require("@sugarcube/utils");
const { promisify } = require("util");
const { cleanUp, existsP } = require("@sugarcube/plugin-fs");
const { parse, fromUnixTime, format, endOfDay, addWeeks, eachWeekOfInterval } = require("date-fns");
const { once } = require('events');
const { createReadStream } = require('fs');
const { createInterface } = require('readline');

const { retry, flatmapP } = dashp;

const querySource = "twitter_user";

const formatDate = date => format(date, "yyyy-MM-dd HH:mm:ss");

async function processTwintTmpFile(file) {
  const tweets = [];

  const rl = createInterface({
    input: createReadStream(file),
    crlfDelay: Infinity
  });

  rl.on("line", (line) => {
    tweets.push(JSON.parse(line));
  });

  await once(rl, "close");

  return tweets;
};

const parseTwitterUser = user => {
  if (Number.isInteger(user)) return user.toString();
  if (user.startsWith("http")) {
    const u = new URL(user);
    return u.pathname
      .replace(/^\//, "")
      .replace(/\/$/, "")
      .split("/")[0];
  }
  return user.replace(/^@/, "");
};

const twintEntity = (user) => ({
  id,
  name,
  username,
  user_id,
  created_at,
  tweet,
  photos,
  video,
  hashtags,
  link: href,
  retweets_count,
  likes_count,
  lang,
  place,
  geo,
}) => {
  const images = photos.map((url) => ({type: "image", term: url}));
  const videos = video === 1 ? [{type: "video", term: href}] : [];
  const urls = [{type: "url", term: href}];
  const createdAt = fromUnixTime(created_at/1000);

  const user = {
    name,
    screen_name: username,
    user_id: user_id.toString(),
  };

  return {
    _sc_id_fields: ["tweet_id"],
    _sc_content_fields: ["tweet"],
    _sc_media: images.concat(videos).concat(urls),
    _sc_pubdates: {
      source: createdAt,
    },
    _sc_queries: [
      {type: querySource, term: user},
    ],
    tweet_id: id.toString(),
    tweet_time: createdAt,
    geo: geo === "" ? null : geo,
    place: place === "" ? null : place,
    lang: lang === "" ? null : lang,
    hashtags: hashtags.map((tag) => ({tag: tag.toLowerCase(), original_tag: tag.replace(/^#/, "")})),
    tweet,
    href,
    retweet_count: retweets_count == null ? 0 : retweets_count,
    favorite_count: likes_count == null ? 0 : likes_count,
    user,
  };
};

async function twitterScraperByInterval(cmd, user, sinceDay, untilDay) {
  const since = formatDate(sinceDay);
  const until = formatDate(untilDay);
  const tempDir = os.tmpdir();
  const tmpFile = `${tempDir}/tweets-${user}-${since}-${until}.json`;
  let data = [];

  try {
    await runCmd(cmd, [
      "-u",
      user,
      "--since",
      since,
      "--until",
      until,
      "-o",
      tmpFile,
      "--json"
    ]);
  } catch (e) {
    if (await existsP(tmpFile)) {
      await cleanUp(tmpFile);
    }
    throw e;
  }

  if (await existsP(tmpFile)) {
    data = await processTwintTmpFile(tmpFile);
    await cleanUp(tmpFile);
  }
  return data;
}

async function twitterScraperFullProfile(cmd, user) {
  const tempDir = os.tmpdir();
  const tmpFile = `${tempDir}/tweets-${user}.json`;
  let data = [];

  await runCmd(cmd, [
    "-u",
    user,
    "--profile-full",
    "-o",
    tmpFile,
    "--json"
  ]);

  if (await existsP(tmpFile)) {
    data = await processTwintTmpFile(tmpFile);
    await cleanUp(tmpFile);
  }

  return data;
};

const plugin = async (envelope, { stats, log, cfg }) => {
  const twintStrategy = get("twitter.twint_strategy", cfg);
  const cmd = get("twitter.twint_cmd", cfg);
  const parallel = get("twitter.twint_interval_parallel", cfg);

  const queries = env.queriesByType(querySource, envelope);

  // Choose the right flatmapPX function, based on the configured parallelism.
  let mod;
  switch (parallel) {
    case parallel <= 1 ? parallel : null:
      mod = "";
      break;
    case parallel > 8 ? parallel : null:
      mod = 8;
      break;
    default:
      mod = parallel;
  }

  const mapper = dashp[`flatmapP${mod}`];

  const intervalStrategy = async (user) => {
    const endDay = endOfDay(new Date());
    const startDay = parse("2011-01-01", "yyyy-MM-dd", new Date());

    const intervals = eachWeekOfInterval({ start: startDay, end: endDay })
      .map((week) => [week, addWeeks(week, 1)]);

    const logCounter = counter(intervals.length, ({cnt, total, percent}) =>
      log.debug(`Progress intervals for ${user}: ${cnt}/${total} intervals (${percent}%).`),
      {threshold: 1, steps: 10},
    );

    return mapper(async ([since, until]) => {
      let data = [];

      try {
        data = await retry(twitterScraperByInterval(cmd, user, since, until));
        logCounter();
      } catch (e) {
        log.warn(
          `${user}: Failure to scrape interval: ${e.message} (${formatDate(since)} till ${formatDate(until)}). Ignoring.`
        );
      }

      return data;
    }, intervals);
  };

  const fullProfileStrategy = (user) => {
    return retry(twitterScraperFullProfile(cmd, user));
  };

  const queryCounter = counter(queries.length, ({cnt, total, percent}) =>
    log.debug(`Progress: ${cnt}/${total} intervals (${percent}%).`),
    {threshold: 1, steps: 5},
  );

  const data = await flatmapP(async query => {
    const user = parseTwitterUser(query);

    let tweets = [];

    stats.count("total");

    let strategy = ["auto", "interval", "profile"].includes(twintStrategy) ? twintStrategy : "auto";

    // If strategy is set to auto, determine the number of tweets and set the
    // strategy appropriately. Always use interval as a default strategy.
    if (twintStrategy === "auto") {
      strategy = "interval";

      try {
        const resp = await fetch(`https://twitter.com/${user}`);
        const html = await resp.text();
        const $ = cheerio.load(html);
        const countValue = $(".ProfileNav-value").data("count");
        if (countValue != null && countValue !== "" && parseInt(countValue, 10) <= 3100) {
          strategy = "profile";
        }
      } catch (e) {
        log.error(`Setting strategy failed: ${e.message}. Using interval as default.`);
      }
    }

    log.info(`Scraping ${user} with strategy: ${strategy}.`);

    // Fetch the tweets for the chosen strategy.
    try {
      if (strategy === "profile") {
        tweets = await fullProfileStrategy(user);
      } else {
        tweets = await intervalStrategy(user);
      }
    } catch (e) {
      log.error(`Failed to scrape ${user}: ${e.message}`);
      stats.fail({ term: query, reason: e.message });
      return [];
    }

    log.info(`Scraped in total ${tweets.length} tweets for ${user}.`);
    stats.count("success");
    stats.count("fetched", tweets.length);
    queryCounter();

    return tweets.map(twintEntity(user));
  }, queries);

  return env.concatData(data, envelope);
};

plugin.argv = {
  "twitter.twint_strategy": {
    type: "string",
    nargs: 1,
    default: "auto",
    desc: "Set the scraping strategy for Twint. Can be auto, interval or profile.",
  },
  "twitter.twint_cmd": {
    type: "string",
    nargs: 1,
    default: "twint",
    desc: "The path to the twint command.",
  },
  "twitter.twint_interval_parallel": {
    type: "number",
    nargs: 1,
    desc:
    "Specify the number of parallel twint interval scrapes. Can be between 1 and 8.",
    default: 8,
  },
};
plugin.desc = "Scrap twitter profiles";

module.exports = plugin;
