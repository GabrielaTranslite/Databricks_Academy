output "databricks_host" {
  value = "https://${data.azurerm_databricks_workspace.prod.workspace_url}"
}

output "adls_dfs_endpoint" {
  value = data.azurerm_storage_account.adls.primary_dfs_endpoint
}

output "key_vault_uri" {
  value = data.azurerm_key_vault.kv.vault_uri
}

output "eventhub_namespace_id" {
  value = data.azurerm_eventhub_namespace.eh.id
}