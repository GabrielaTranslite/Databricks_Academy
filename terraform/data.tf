# Existing Azure resources, read-only (data sources)

data "azurerm_resource_group" "rg" {
  name = "PL_24_Databricks"
}

data "azurerm_databricks_workspace" "prod" {
  name                = "dbr_dev"
  resource_group_name = data.azurerm_resource_group.rg.name
}

data "azurerm_storage_account" "adls" {
  name                = "dlspl21databricks"
  resource_group_name = data.azurerm_resource_group.rg.name
}

data "azurerm_key_vault" "kv" {
  name                = "kvpl24databricks2"
  resource_group_name = data.azurerm_resource_group.rg.name
}

data "azurerm_eventhub_namespace" "eh" {
  name                = "evhpl24databricks02"
  resource_group_name = data.azurerm_resource_group.rg.name
}