require("dotenv").config();

const { Client: elasticSearchClient } = require("@elastic/elasticsearch");
const { Client: postgresClient } = require("pg");
const Progress = require("cli-progress");

const postgres = new postgresClient({
  user: process.env.PG_USER,
  host: process.env.PG_HOST,
  database: process.env.PG_DATABASE,
  password: process.env.PG_PASSWORD,
  port: process.env.PG_PORT,
});
const elasticSearch = new elasticSearchClient({ node: process.env.ES_NODE });

async function run() {
  await postgres.connect();

  const { rows: locations } = await postgres.query(`SELECT * FROM "tenant"`);

  for (const location of locations) {
    const { rows: boulders } = await postgres.query(
      `SELECT * FROM "boulder" WHERE tenant_id = ${location.id}`
    );
    const { rows: grades } = await postgres.query(
      `SELECT * FROM "grade" WHERE tenant_id = ${location.id}`
    );
    const { rows: walls } = await postgres.query(
      `SELECT * FROM "wall" WHERE tenant_id = ${location.id}`
    );
    const { rows: holdTypes } = await postgres.query(
      `SELECT * FROM "hold_color" WHERE tenant_id = ${location.id}`
    );

    const bar = new Progress.SingleBar({}, Progress.Presets.shades_classic);
    bar.start(boulders.length, 0);

    for (let boulder of boulders) {
      const { rows: boulderSetters } = await postgres.query(
        `SELECT * FROM "boulder_setters_v2" INNER JOIN "setter" ON boulder_setters_v2.setter_id = setter.id WHERE boulder_id = ${boulder.id}`
      );
      const { rows: boulderTags } = await postgres.query(
        `SELECT * FROM "boulder_tags" INNER JOIN "tag" ON boulder_tags.tag_id = tag.id WHERE boulder_id = ${boulder.id}`
      );

      boulder.setters = boulderSetters;
      boulder.tags = boulderTags;
      boulder.grade = grades.find((grade) => grade.id === boulder.grade_id);
      boulder.internal_grade = grades.find(
        (grade) => grade.id === boulder.internal_grade_id
      );
      boulder.start_wall = walls.find(
        (wall) => wall.id === boulder.start_wall_id
      );
      boulder.end_wall = walls.find((wall) => wall.id === boulder.end_wall_id);
      boulder.hold_type = holdTypes.find(
        (holdType) => holdType.id === boulder.color_id
      );
      boulder.location_id = boulder.tenant_id;

      delete boulder.color_id;
      delete boulder.internal_grade_id;
      delete boulder.start_wall_id;
      delete boulder.end_wall_id;
      delete boulder.grade_id;
      delete boulder.grade_id;
      delete boulder.tenant_id;

      boulder.setters = boulder.setters.map((boulderSetter) => ({
        id: boulderSetter.setter_id,
        username: boulderSetter.username,
        active: boulderSetter.active,
      }));

      boulder.tags = boulder.tags.map((boulderTag) => ({
        id: boulderTag.id,
        name: boulderTag.name,
        emoji: boulderTag.emoji,
      }));

      boulder.grade = {
        id: boulder.grade.id,
        name: boulder.grade.name,
      };

      boulder.internal_grade = {
        id: boulder.internal_grade.id,
        name: boulder.internal_grade.name,
      };

      boulder.start_wall = {
        id: boulder.start_wall.id,
        name: boulder.start_wall.name,
      };

      boulder.hold_type = {
        id: boulder.hold_type.id,
        name: boulder.hold_type.name,
      };

      if (boulder.end_wall) {
        boulder.end_wall = {
          id: boulder.end_wall.id,
          name: boulder.end_wall.name,
        };
      }

      bar.increment();

      // Let's start by indexing some data
      await elasticSearch.index({
        index: `boulder-${location.url}`,
        id: boulder.id,
        body: boulder,
      });
    }

    bar.stop();
  }

  await postgres.end();
}

run().catch(console.log);
