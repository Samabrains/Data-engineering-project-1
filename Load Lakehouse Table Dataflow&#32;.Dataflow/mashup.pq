[DefaultOutputDestinationSettings = [DestinationDefinition = [Kind = "Reference", QueryName = "DefaultDestination", IsNewTarget = true], UpdateMethod = [Kind = "Replace"], DestinationTypeSettings = [Kind = "Table"]], StagingDefinition = [Kind = "FastCopy"]]
section Section1;
[BindToDefaultDestination = true]
shared dimension_customer = let
  Source = GoogleSheets.Contents("https://docs.google.com/spreadsheets/d/1cERubXUMPCddxAx4z38s3SRd1UCO1uNv7B8eHGPqIck/edit"),
  #"Navigation 1" = Source{[name = "dimension_customer", ItemKind = "Table"]}[Data],
  #"Promoted headers" = Table.PromoteHeaders(#"Navigation 1", [PromoteAllScalars = true]),
  #"Changed column type" = Table.TransformColumnTypes(#"Promoted headers", {{"CustomerKey", Int64.Type}, {"WWICustomerID", Int64.Type}, {"Customer", type text}, {"BillToCustomer", type text}, {"Category", type text}, {"BuyingGroup", type text}, {"PrimaryContact", type text}, {"ValidFrom", type datetime}, {"ValidTo", type datetime}, {"LineageKey", Int64.Type}}),
  #"Removed top rows" = Table.Skip(#"Changed column type", 1)
in
  #"Removed top rows";
shared DefaultDestination = let
  Source = Lakehouse.Contents([HierarchicalNavigation = null, CreateNavigationProperties = false, EnableFolding = false]),
  #"Navigation 1" = Source{[workspaceId = "189641ee-9dee-49bf-a77f-366a38d83a82"]}[Data],
  #"Navigation 2" = #"Navigation 1"{[lakehouseId = "dc7fe9a3-4f83-41f2-adc8-7cd78a7bfcda"]}[Data]
in
  #"Navigation 2";
