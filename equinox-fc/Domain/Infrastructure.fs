namespace global

open FSharp.UMX // see https://github.com/fsprojects/FSharp.UMX - % operator and ability to apply units of measure to Guids+strings

(* Locations *)

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

type InventoryEpochId = int<inventoryEpochId>
and [<Measure>] inventoryEpochId
module InventoryEpochId =
    let parse (value : int) : InventoryEpochId = %value
    let next (value : InventoryEpochId) : InventoryEpochId = % (%value + 1)
    let toString (value : InventoryEpochId) : string = string %value

type InventoryTransactionId = string<inventoryTransactionId>
and [<Measure>] inventoryTransactionId
module InventoryTransactionId =
    let parse (value : string) : InventoryTransactionId = %value
    let (|Parse|) = parse
    let toString (value : InventoryTransactionId) : string = %value

(* Tickets *)

type TicketId = string<ticketId>
and [<Measure>] ticketId
module TicketId =
    let parse (value : string) : TicketId = let raw = value in %raw
    let toString (value : TicketId) : string = %value

type TicketListId = string<ticketListId>
and [<Measure>] ticketListId
module TicketListId =
    let parse (value : string) : TicketListId = let raw = value in %raw
    let toString (value : TicketListId) : string = %value

type AllocationId = string<allocationId>
and [<Measure>] allocationId
module AllocationId =
    let parse (value : string) : AllocationId = let raw = value in %raw
    let toString (value : AllocationId) : string = %value

type AllocatorId = string<allocatorId>
and [<Measure>] allocatorId
module AllocatorId =
    let parse (value : string) : AllocatorId = let raw = value in %raw
    let toString (value : AllocatorId) : string = %value