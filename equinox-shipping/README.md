# Shipping / Process Manager / Watchdog example

NOTE: In general, this is not intended to be the first example you touch when exploring Equinox (see https://github.com/jet/equinox#quickstart for a more progressive sequence of things to explore)

This project was generated using:

    dotnet new -i Equinox.Templates # just once, to install/update in the local templates store
    dotnet new eqxShipping

The purpose of this template is to demonstrate the implementation of a [Process Manager](https://www.enterpriseintegrationpatterns.com/patterns/messaging/ProcessManager.html) using [`Equinox`](https://github.com/jet/equinox) that manages the enlistment of a set of `Shipment` Aggregate Root items into a separated `Container` Aggregate Root as an atomic operation.

Implementation notes: 
   - processing is fully idempotent; retries, concurrent or overlapping transactions are intended to be handled thoroughly and correctly
   - if any `Shipment`s cannot be `Reserved`, those that have been get `Revoked`, and the failure is reported to the caller
   - includes a `Watchdog` console app (based on `dotnet new proReactor --source changeFeedOnly --blank`) responsible for `Drive`ing any abandoned / stuck transactions neglected by e.g. processing within a HTTP request that times out and is not retried by the client through to their conclusion.

## Exercise / hole

A remaining hole is that multiple concurrent Finalization transactions are not prevented (which I believe is different from the actual app semantics at the present time - Finalization is intended to be a final act 😸). It may be a interesting exercise for the reader to add processing to the FinalizationTransaction (and Container) such that one or more of the following behaviors apply: 
- Gate the processing via a reservation on the container (you'll definitely need a Watchdog!)
- Do an optimistic pre-flight check, visiting the Container to see if it's already Finalized
- If one reaches the Finalize action but determine it has already been completed by a competing writer, switch to a reverting mode (but that would open the possibility of multiple Assigned events)
- Register the Finalization intent on the Container once the Reservations have been attained, then mark it Finalized after that

## Usage instructions

See Watchdog/README.md for some minimal setup instructions.
