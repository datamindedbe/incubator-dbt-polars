// @ts-check

/** @type {import('@docusaurus/plugin-content-docs').SidebarsConfig} */
const sidebars = {
  docs: [
    {
      type: 'category',
      label: 'Getting started',
      collapsible: false,
      className: 'sidebar-section',
      items: [
        {type: 'doc', id: 'index', label: 'Introduction'},
        'quickstart',
        {
          type: 'category',
          label: 'Demo project',
          items: [
            'tutorial/loading-data',
            'tutorial/transforming-data',
            'tutorial/ephemeral-models',
            'tutorial/documentation',
            'tutorial/data-tests',
            'tutorial/incremental-ingestion',
          ],
        },
      ],
    },
    {
      type: 'category',
      label: 'Essentials',
      collapsible: false,
      className: 'sidebar-section',
      items: [
        {
          type: 'category',
          label: 'Connecting to a catalog',
          link: {type: 'doc', id: 'guides/connecting-to-a-catalog'},
          items: [
            'guides/catalogs/azure',
            {
              type: 'category',
              label: 'Databricks',
              link: {type: 'doc', id: 'guides/catalogs/databricks'},
              items: ['guides/catalogs/databricks-external-tables'],
            },
            'guides/catalogs/fabric',
            'guides/catalogs/snowflake',
            'guides/catalogs/iceberg',
          ],
        },
        'guides/source-data',
        'guides/python-models',
        'guides/testing',
      ],
    },
    {
      type: 'category',
      label: 'Advanced topics',
      collapsible: false,
      className: 'sidebar-section',
      items: ['guides/performance', 'guides/combining-catalogs', 'guides/custom-catalog'],
    },
    {
      type: 'category',
      label: 'Reference',
      collapsible: false,
      className: 'sidebar-section',
      items: [
        {
          type: 'category',
          label: 'Catalogs',
          items: [
            'catalogs/azure',
            'catalogs/databricks',
            'catalogs/fabric',
            'catalogs/iceberg',
            'catalogs/local',
            'catalogs/snowflake',
          ],
        },
        'model-configuration',
        'dbt-compatibility',
      ],
    },
  ],
};

module.exports = sidebars;
