// @ts-check

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'dbt-polars',
  tagline: 'dbt adapter for Polars',
  url: 'https://datamindedbe.github.io',
  baseUrl: '/incubator-dbt-polars/',
  organizationName: 'datamindedbe',
  projectName: 'incubator-dbt-polars',
  trailingSlash: false,

  onBrokenLinks: 'throw',

  markdown: {
    // Parse .md as plain CommonMark rather than MDX, so `{` and `<` in prose need no escaping
    format: 'detect',
  },

  presets: [
    [
      'classic',
      /** @type {import('@docusaurus/preset-classic').Options} */
      ({
        docs: {
          path: '../docs',
          routeBasePath: '/',
          sidebarPath: './sidebars.js',
        },
        blog: false,
      }),
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      navbar: {
        title: 'dbt-polars',
        items: [
          {
            href: 'https://github.com/datamindedbe/incubator-dbt-polars',
            label: 'GitHub',
            position: 'right',
          },
        ],
      },
    }),
};

module.exports = config;
