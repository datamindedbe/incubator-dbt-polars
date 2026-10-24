// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docs: [
    'index',
    'demos/getting_started',
    {
      type: 'category',
      label: 'Catalogs',
      link: {type: 'doc', id: 'catalog-overview'},
      items: [
        {
          type: 'category',
          label: 'Available catalogs',
          collapsed: false,
          items: [
            'catalogs/azure',
            'catalogs/databricks',
            'catalogs/fabric',
            'catalogs/iceberg',
            'catalogs/local',
            'catalogs/snowflake',
          ],
        },
        'catalogs/custom',
      ],
    },
    'python-models',
    'dbt-compatibility',
  ],
};

module.exports = sidebars;
