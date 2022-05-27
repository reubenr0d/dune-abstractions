{% macro alter_tblproperties_curvefi_trades() -%}
{%- if target.name == 'prod'-%}
ALTER VIEW curvefi.trades SET TBLPROPERTIES('dune.public'='true',
                                                    'dune.data_explorer.blockchains'='["ethereum"]',
                                                    'dune.data_explorer.category'='abstraction',
                                                    'dune.data_explorer.abstraction.type'='project',
                                                    'dune.data_explorer.abstraction.name'='curvefi',
                                                    'dune.data_explorer.contributors'='["yulesa","soispoke"]');
{%- else -%}
{%- endif -%}
{%- endmacro %}
