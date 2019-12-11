const os = require("os");
const { URL } = require("url");
const { retry, flatmapP } = require("dashp");
const { envelope: env } = require("@sugarcube/core");
const { runCmd } = require("@sugarcube/utils");
const { promisify } = require("util");
const { cleanUp, existsP } = require("@sugarcube/plugin-fs");
const { parse, fromUnixTime } = require("date-fns");
const { once } = require('events');
const { createReadStream } = require('fs');
const { createInterface } = require('readline');

const querySource = "twitter_user";

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

async function twitterScraper(user) {
  const tempDir = os.tmpdir();
  const tmpFile = `${tempDir}/tweets-${user}.csv`;
  let data = [];

  await runCmd(
    "twint",
    ["-u", user, "--profile-full", "-o", tmpFile, "--json"]
  );

  if (await existsP(tmpFile)) {
    data = await processTwintTmpFile(tmpFile);
    await cleanUp(tmpFile);
  }
  return data;
}

const plugin = async (envelope, { stats, log }) => {
  const queries = env
    .queriesByType(querySource, envelope);

  const data = await flatmapP(async query => {
    stats.count("total");
    let tweets;
    log.info(query);
    try {
      tweets = await twitterScraper(parseTwitterUser(query));
    } catch (e) {
      log.error(`Failed to scrape ${query}: ${e.message}`);
      stats.fail({ term: query, reason: e.message });
      return [];
    }

    stats.count("success");

    log.info(`We have scraped ${tweets.length} tweets for ${query}.`);

    return tweets.map(
      ({
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
            {type: querySource, term: query},
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
      }
    );
  }, queries);

  return env.concatData(data, envelope);
};

plugin.argv = {};
plugin.desc = "Scrap twitter profiles";

module.exports = plugin;
