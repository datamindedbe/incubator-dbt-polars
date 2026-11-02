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

  clientModules: [require.resolve('./src/prism-python-annotations.js')],

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
        theme: {
          customCss: './src/css/custom.css',
        },
      }),
    ],
  ],

  themes: [
    [
      require.resolve('@easyops-cn/docusaurus-search-local'),
      {
        hashed: true,
        docsRouteBasePath: '/',
        indexBlog: false,
        highlightSearchTermsOnTargetPage: true,
      },
    ],
  ],

  themeConfig:
    /** @type {import('@docusaurus/preset-classic').ThemeConfig} */
    ({
      prism: {
        theme: require('prism-react-renderer').themes.github,
        darkTheme: require('prism-react-renderer').themes.dracula,
        additionalLanguages: ['hcl'],
      },
      navbar: {
        title: 'dbt-polars',
        items: [
          {
            href: 'https://github.com/datamindedbe/incubator-dbt-polars',
            label: 'GitHub',
            position: 'right',
            className: 'navbar-github-link',
          },
        ],
      },
    }),
};

module.exports = config;
