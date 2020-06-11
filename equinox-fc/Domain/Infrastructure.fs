namespace global

open FSharp.UMX // see https://github.com/fsprojects/FSharp.UMX - % operator and ability to apply units of measure to Guids+strings

type LocationId = string<locationId>
and [<Measure>] locationId
module LocationId =
    let parse (value : string) : LocationId = %value
    let toString (value : LocationId) : string = %value

type LocationEpochId = int<locationEpochId>
and [<Measure>] locationEpochId
module LocationEpochId =
    let parse (value : int) : LocationEpochId = %value
    let next (value : LocationEpochId) : LocationEpochId = % (%value + 1)
    let toString (value : LocationEpochId) : string = string %value

type InventoryId = string<inventoryId>
and [<Measure>] inventoryId
module InventoryId =
    let parse (value : string) : InventoryId = %value
    let toString (value : InventoryId) : string = %value

type InventoryTransactionId = string<inventoryTransactionId>
and [<Measure>] inventoryTransactionId
module InventoryTransactionId =
    let parse (value : string) : InventoryTransactionId = %value
    let (|Parse|) = parse
    let toString (value : InventoryTransactionId) : string = %value
