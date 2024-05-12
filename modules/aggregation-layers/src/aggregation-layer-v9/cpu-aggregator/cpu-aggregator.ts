import type {Aggregator, AggregationProps} from '../aggregator';
import {_deepEqual as deepEqual, BinaryAttribute} from '@deck.gl/core';
import {aggregateWeights, sortBins, packBinIds} from './utils';
import type {NumberArray} from '@luma.gl/core';

/** Settings used to construct a new CPUAggregator */
export type CPUAggregatorSettings = {
  /** Size of bin IDs */
  dimensions: number;
  /** Attribute id that provides input to getBin, used to index into CPUAggregationProps.attributes during update.
   * For example `coordinate`
   */
  binSource?: string;
  /** Callback to map data point to a bin id.
   * If dimensions=1, should return a number;
   * If dimensions>1, should return an array with [dimensions] elements;
   * Return null if the data point should be skipped.
   */
  getBinId: (
    /** Data is retrieved from the attribute with id={binSource}, or null if not available */
    data: NumberArray | undefined,
    /** Index of the data point */
    index: number,
    /** CPUAggregationProps.binOptions */
    options: Record<string, any>
  ) => number | number[] | null;
  /** Attribute id that provides weights for each channel, used to index into CPUAggregationProps.attributes during update.
   * For example `['observations', 'temperature']`
   */
  weightSources: string[];
};

/** Options used to run CPU aggregation, can be changed at any time */
export type CPUAggregationProps = AggregationProps & {};

export type Bin = {
  id: number | number[];
  index: number;
  /** list of data point indices */
  points: number[];
};

/** An Aggregator implementation that calculates aggregation on the CPU */
export class CPUAggregator implements Aggregator {
  dimensions: number;
  numChannels: number;
  binSource: string | undefined;
  weightSources: string[];

  props: CPUAggregationProps = {
    binOptions: {},
    pointCount: 0,
    operations: [],
    attributes: {}
  };

  protected getBinId: CPUAggregatorSettings['getBinId'];
  /** Dirty flag
   * If true, redo sorting
   * If array, redo aggregation on the specified channel
   */
  protected needsUpdate: boolean[] | boolean;

  protected bins: Bin[] = [];
  protected binIds: Float32Array | null = null;
  protected results: {value: Float32Array; domain: [min: number, max: number]}[] = [];

  constructor({dimensions, binSource, weightSources, getBinId}: CPUAggregatorSettings) {
    this.dimensions = dimensions;
    this.numChannels = weightSources.length;
    this.binSource = binSource;
    this.weightSources = weightSources;
    this.needsUpdate = true;
    this.getBinId = getBinId;
  }

  get numBins() {
    return this.bins.length;
  }

  /** Update aggregation props */
  setProps(props: Partial<CPUAggregationProps>) {
    const oldProps = this.props;

    if (props.binOptions) {
      if (!deepEqual(props.binOptions, oldProps.binOptions, 2)) {
        this.setNeedsUpdate();
      }
    }
    if (props.operations) {
      for (let channel = 0; channel < this.numChannels; channel++) {
        if (props.operations[channel] !== oldProps.operations[channel]) {
          this.setNeedsUpdate(channel);
        }
      }
    }
    if (props.pointCount !== undefined && props.pointCount !== oldProps.pointCount) {
      this.setNeedsUpdate();
    }
    if (props.attributes) {
      props.attributes = {...oldProps.attributes, ...props.attributes};
    }
    Object.assign(this.props, props);
  }

  /** Flags a channel to need update
   * This is called internally by setProps() if certain props change
   * Users of this class still need to manually set the dirty flag sometimes, because even if no props changed
   * the underlying buffers could have been updated and require rerunning the aggregation
   * @param {number} channel - mark the given channel as dirty. If not provided, all channels will be updated.
   */
  setNeedsUpdate(channel?: number) {
    if (channel === undefined) {
      this.needsUpdate = true;
    } else if (this.needsUpdate !== true) {
      this.needsUpdate = this.needsUpdate || [];
      this.needsUpdate[channel] = true;
    }
  }

  /** Run aggregation */
  update() {
    if (this.needsUpdate === true) {
      const attribute = this.props.attributes[this.binSource!] ?? null;
      this.bins = sortBins({
        pointCount: this.props.pointCount,
        attribute,
        getBinId: this.getBinId,
        options: this.props.binOptions
      });
      this.binIds = packBinIds({
        bins: this.bins,
        dimensions: this.dimensions,
        target: this.binIds
      });
    }
    for (let channel = 0; channel < this.numChannels; channel++) {
      if (this.needsUpdate === true || this.needsUpdate[channel]) {
        const attributeName = this.weightSources[channel];
        const attribute = this.props.attributes[attributeName];
        if (!attribute) {
          throw new Error(`Cannot find attribute ${attributeName}`);
        }
        this.results[channel] = aggregateWeights({
          bins: this.bins,
          attribute,
          operation: this.props.operations[channel],
          target: this.results[channel]?.value
        });
      }
    }
  }

  /** Returns an accessor to the bins. */
  getBins(): BinaryAttribute | null {
    if (!this.binIds) {
      return null;
    }
    return {value: this.binIds, type: 'float32', size: this.dimensions};
  }

  /** Returns an accessor to the output for a given channel. */
  getResult(channel: number): BinaryAttribute | null {
    const result = this.results[channel];
    if (!result) {
      return null;
    }
    return {value: result.value, type: 'float32', size: 1};
  }

  /** Returns the [min, max] of aggregated values for a given channel. */
  getResultDomain(channel: number): [min: number, max: number] {
    return this.results[channel]?.domain ?? [Infinity, -Infinity];
  }

  /** Returns the information for a given bin. */
  getBin(index: number): {
    /** The original id */
    id: number | number[];
    /** Aggregated values by channel */
    value: number[];
    /** Count of data points in this bin */
    count: number;
    /** List of data point indices that fall into this bin. */
    points?: number[];
  } | null {
    const bin = this.bins[index];
    if (!bin) {
      return null;
    }
    const value = new Array(this.numChannels);
    for (let i = 0; i < value.length; i++) {
      const result = this.results[i];
      value[i] = result?.value[index];
    }
    return {
      id: bin.id,
      value,
      count: bin.points.length,
      points: bin.points
    };
  }
}
