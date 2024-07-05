/// TODO: Some functions in this module are liable to race conditions.

import Buffer "mo:base/Buffer";
import Region "mo:base/Region";
import Array "mo:base/Array";
import Principal "mo:base/Principal";

module {
  public type StorageIndex = {
    var perCanister: Nat;
    var partitions: Buffer.Buffer<PartitionCanister>;
  };

  public type SubRegion = {
    offset: Nat64;
    size: Nat;
  };

  public type StoragePartition = {
    region: Region.Region;
    subRegions: Buffer.Buffer<SubRegion>;
  };

  public type IndexCanister = actor {
    createPartition(): async Principal;
  };

  public type PartitionCanister = actor {
    subRegions(): async [SubRegion];
  };

  public func createStorage({perCanister: Nat}): StorageIndex {
    { var perCanister; var partitions = Buffer.Buffer(1) };
  };

  public func getPartitions(index: StorageIndex): [PartitionCanister] {
    Buffer.toArray(index.partitions);
  };

  public func getLastPartition(index: StorageIndex): PartitionCanister {
    getPartitions(index)[index.partitions.size() - 1];
  };

  public func addNewPartition(indexCanister: IndexCanister, index: StorageIndex): async* Principal {
    let partition = await indexCanister.createPartition();
    index.partitions.add(actor(Principal.toText(partition)): PartitionCanister);
    partition;
  };

  public func addBlob(indexCanister: IndexCanister, index: StorageIndex, blob: Blob): async () {
    let lastPartition = getLastPartition(index);
    let (ourPartition, ourSubRegions) = do {
      let subRegions = await lastPartition.subRegions();
      if (subRegions.size() >= index.perCanister) {
        let newPartition: PartitionCanister = actor(Principal.toText(addNewPartition(indexCanister, index)));
        (newPartition, await newPartition.subRegions());
      } else {
        (lastPartition, subRegions);
      };
      let offset = if (subRegions.size() == 0) {
        0
      } else {
        Buffer.toArray<SubRegion>(subRegions)[subRegions.size() - 1];
      };
      Region.storeBlob(index.region, offset, blob);
      subRegions.add(index.region); // FIXME: It doesn't persist to stable memory.
    };
  };

  public func getBlob(index: StoragePartition, n: Nat): Blob {
    let subRegion = Buffer.toArray(subRegions)[n];
    Region.loadBlob(index.region, subRegion.offset, subRegion.size);
  };
};
