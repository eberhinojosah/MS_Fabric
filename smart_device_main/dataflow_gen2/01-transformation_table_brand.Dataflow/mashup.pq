[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "brand_DataDestination", IsNewTarget = true], Settings = [Kind = "Manual", AllowCreation = true, ColumnSettings = [Mappings = {[SourceColumnName = "brand_id", DestinationColumnName = "brand_id"], [SourceColumnName = "brand", DestinationColumnName = "brand"], [SourceColumnName = "hardware_designer", DestinationColumnName = "hardware_designer"], [SourceColumnName = "manufacturer", DestinationColumnName = "manufacturer"], [SourceColumnName = "ingestion_date", DestinationColumnName = "ingestion_date"], [SourceColumnName = "file_date", DestinationColumnName = "file_date"]}], DynamicSchema = true, UpdateMethod = [Kind = "Replace"], TypeSettings = [Kind = "Table"]]]}]
shared brand = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navigation 1" = Source{[workspaceId = "44d09e17-8f0e-43a0-bbb6-a600b3cbd2bb"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7ef17035-eddb-4f03-bdd9-a978e296af24"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "brand", ItemKind = "Table"]}[Data],
  #"Replaced value" = Table.ReplaceValue(#"Navigation 3", "", "unknow", Replacer.ReplaceValue, {"hardware_designer"}),
  #"Replaced value 1" = Table.ReplaceValue(#"Replaced value", "", "unknow", Replacer.ReplaceValue, {"manufacturer"}),
  #"Removed columns" = Table.RemoveColumns(#"Replaced value 1", {"environment"}),
  #"Replaced value 2" = Table.ReplaceValue(#"Removed columns", "unknow", "unknown", Replacer.ReplaceText, {"hardware_designer"}),
  #"Replaced value 3" = Table.ReplaceValue(#"Replaced value 2", "unknow", "unknown", Replacer.ReplaceText, {"manufacturer"})
in
  #"Replaced value 3";
shared brand_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "44d09e17-8f0e-43a0-bbb6-a600b3cbd2bb"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "0441ae0c-df2d-46f2-99f9-aa3e4e65098b"]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_brand", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
