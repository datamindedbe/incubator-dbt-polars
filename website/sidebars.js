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
        'catalogs/local',
        'catalogs/azure',
        'catalogs/iceberg',
        'catalogs/databricks',
        'catalogs/custom',
      ],
    },
    'python-models',
    'sql-macros',
  ],
};

module.exports = sidebars;
