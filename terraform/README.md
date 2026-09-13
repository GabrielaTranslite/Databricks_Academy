# Terraform - Azure infrastructure layer

Read-only Terraform configuration that inventories the Azure side of the platform. It does not create
or modify anything: it uses `data` sources to read the existing resources and exposes their
coordinates as outputs. This is the infrastructure layer of the two-layer model - Terraform describes
the infrastructure, the Databricks Asset Bundle (see `../databricks.yml`) deploys the assets on it.

## What it reads

- Resource group `PL_24_Databricks`
- Databricks workspace `dbr_dev` (outputs the workspace host used by the bundle)
- Storage account, ADLS Gen2 (outputs the DFS endpoint)
- Key Vault (outputs the vault URI)
- Event Hubs namespace (outputs the resource id)

## Run it

```powershell
az login
terraform init
terraform apply -auto-approve      # data-only; reports 0 added, 0 changed, 0 destroyed
terraform output                   # prints the coordinates
terraform output -raw databricks_host
```

## Notes

- Authentication is your Azure CLI login (`az login`); the `azurerm` provider picks it up.
- Outputs expose only non-sensitive coordinates (endpoints, URIs, ids). Connection strings and keys
  are never output; if one is ever needed, mark that output `sensitive = true`.
- This layer is intentionally NOT wired into `cd.yml`. The deploy path uses the static host from
  `databricks.yml`, so a Terraform or Azure-auth problem can never block a deploy.
