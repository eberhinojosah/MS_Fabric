[StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[DataDestinations = {[Definition = [Kind = "Reference", QueryName = "camera_DataDestination", IsNewTarget = true], Settings = [Kind = "Automatic", TypeSettings = [Kind = "Table"]]]}]
shared camera = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null]),
  #"Navigation 1" = Source{[workspaceId = "44d09e17-8f0e-43a0-bbb6-a600b3cbd2bb"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "7ef17035-eddb-4f03-bdd9-a978e296af24"]}[Data],
  #"Navigation 3" = #"Navigation 2"{[Id = "camera", ItemKind = "Table"]}[Data],
  #"Removed columns" = Table.RemoveColumns(#"Navigation 3", {"environment"})
in
  #"Removed columns";
shared camera_DataDestination = let
  Pattern = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  Navigation_1 = Pattern{[workspaceId = "44d09e17-8f0e-43a0-bbb6-a600b3cbd2bb"]}[Data],
  Navigation_2 = Navigation_1{[lakehouseId = "0441ae0c-df2d-46f2-99f9-aa3e4e65098b"]}[Data],
  TableNavigation = Navigation_2{[Id = "dim_camera", ItemKind = "Table"]}?[Data]?
in
  TableNavigation;
