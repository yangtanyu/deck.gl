import type {Bin} from './cpu-aggregator';
import type {AggregationOperation} from '../aggregator';
import type {Attribute} from '@deck.gl/core';
import type {TypedArray, NumberArray} from '@luma.gl/core';

type AccessorOrValue = ((index: number) => number) | number;
type AggregationFunc = (getValue: AccessorOrValue, pointIndices: number[]) => number;

const AGGREGATION_FUNC: Record<AggregationOperation, AggregationFunc> = {
  SUM: sum,
  MEAN: mean,
  MIN: min,
  MAX: max
} as const;

/**
 * Group data points into bins by a list of binIds
 */
export function sortBins({
  pointCount,
  attribute,
  getBinId,
  options
}: {
  pointCount: number;
  /** Data source */
  attribute: Attribute | null;
  getBinId: (
    data: NumberArray | undefined,
    index: number,
    options: Record<string, any>
  ) => number | number[] | null;
  options: Record<string, number | number[]>;
}): Bin[] {
  const getValue = attribute && getVertexAccessor(attribute);

  const bins: Bin[] = [];
  const binsById: Map<string, Bin> = new Map();

  for (let i = 0; i < pointCount; i++) {
    const id = getBinId(getValue?.(i), i, options);
    if (id === null) {
      continue;
    }
    let bin = binsById.get(String(id));
    if (bin) {
      bin.points.push(i);
    } else {
      bin = {
        id,
        index: bins.length,
        points: [i]
      };
      binsById.set(String(id), bin);
      bins.push(bin);
    }
    i++;
  }
  return bins;
}

export function packBinIds({
  bins,
  dimensions,
  target
}: {
  bins: Bin[];
  /** Size of bin IDs */
  dimensions: number;
  /** Optional typed array to pack values into */
  target: Float32Array | null;
}): Float32Array {
  const targetLength = bins.length * dimensions;
  if (!target || target.length < targetLength) {
    target = new Float32Array(targetLength);
  }
  for (let i = 0; i < bins.length; i++) {
    const {id} = bins[i];
    if (Array.isArray(id)) {
      target.set(id, i * dimensions);
    } else {
      target[i] = id;
    }
  }
  return target;
}

/**
 * Performs aggregation
 * @returns Floa32Array of aggregated values, one for each bin, and the [min,max] of the values
 */
export function aggregateWeights({
  bins,
  attribute,
  operation,
  target
}: {
  bins: Bin[];
  /** The weights of each data point */
  attribute: Attribute;
  /** Type of aggregation */
  operation: AggregationOperation;
  /** Optional typed array to pack values into */
  target?: Float32Array;
}): {
  value: Float32Array;
  domain: [min: number, max: number];
} {
  if (!target || target.length < bins.length) {
    target = new Float32Array(bins.length);
  }
  let min = Infinity;
  let max = -Infinity;
  const getValue = attribute.isConstant ? attribute.value![0] : getVertexAccessor(attribute, 1);

  const aggregationFunc = AGGREGATION_FUNC[operation].bind(null, getValue);

  for (let j = 0; j < bins.length; j++) {
    const {points} = bins[j];
    target[j] = aggregationFunc(points);
    if (target[j] < min) min = target[j];
    if (target[j] > max) max = target[j];
  }

  return {value: target, domain: [min, max]};
}

function sum(getValue: AccessorOrValue, pointIndices: number[]): number {
  if (typeof getValue === 'number') {
    return getValue * pointIndices.length;
  }
  let result = 0;
  for (const i of pointIndices) {
    result += getValue(i);
  }
  return result;
}

function mean(getValue: AccessorOrValue, pointIndices: number[]): number {
  if (pointIndices.length === 0) {
    return NaN;
  }
  return sum(getValue, pointIndices) / pointIndices.length;
}

function min(getValue: AccessorOrValue, pointIndices: number[]): number {
  if (typeof getValue === 'number') {
    return getValue;
  }
  let result = Infinity;
  for (const i of pointIndices) {
    const value = getValue(i);
    if (value < result) {
      result = value;
    }
  }
  return result;
}

function max(getValue: AccessorOrValue, pointIndices: number[]): number {
  if (typeof getValue === 'number') {
    return getValue * pointIndices.length;
  }
  let result = -Infinity;
  for (const i of pointIndices) {
    const value = getValue(i);
    if (value > result) {
      result = value;
    }
  }
  return result;
}

/** Access vertex values from a packed attribute */
function getVertexAccessor(attribute: Attribute, size: 1): (vertexIndex: number) => number;
function getVertexAccessor(
  attribute: Attribute,
  size?: 2 | 3 | 4
): (vertexIndex: number) => TypedArray;
function getVertexAccessor(
  attribute: Attribute,
  size?: number
): ((vertexIndex: number) => number) | ((vertexIndex: number) => TypedArray) {
  size = size ?? attribute.size;
  const value = attribute.value as TypedArray;
  const {offset = 0, stride} = attribute.getAccessor();
  const bytesPerElement = value.BYTES_PER_ELEMENT;
  const elementOffset = offset / bytesPerElement;
  const elementStride = stride ? stride / bytesPerElement : size;
  if (size === 1) {
    return (pointIndex: number) => {
      const i = elementOffset + elementStride * pointIndex;
      return value[i];
    };
  }
  return (pointIndex: number) => {
    const i = elementOffset + elementStride * pointIndex;
    return value.subarray(i, i + size!);
  };
}
