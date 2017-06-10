import Foundation
import Result

/// Represents an atomic batch of changes made to a collection.
///
/// A collection delta contains relative positions of elements within the collection. It
/// is safe to use these offsets directly as indices when `Elements` is statically known
/// to be container types like `Array` and `ContiguousArray`. However, the offsets should
/// not be used directly in a generic context. Refer to the documentations of these
/// offsets for how they should be consumed correctly.
///
/// The change history associated with the first delta received by any given observation
/// is generally irrelevant. Observations should special case the first delta as a
/// complete replacement, or whatever semantic that fits their purpose.
///
/// The family of collection delta operators guarantees that a reference to the previous
/// state of the collection, via `previous`, is always available in the second and later
/// delta received by any given observation.
public struct CollectionDelta<Elements: Collection> {
	/// The collection prior to the changes, or `nil` if the collection has not ever been
	/// changed before.
	///
	/// - important: `previous` is guaranteed not to be `nil` if `self` is the second or
	///              later delta that you have received for a given observation.
	public let previous: Elements?

	/// The collection with the changes applied.
	public let current: Elements

	/// The relative positions of inserted elements **after** the removals were applied.
	/// These are valid only with the `current` snapshot.
	///
	/// - important: To obtain the actual index, you must query the `index(_:offsetBy:)`
	///              method on `current`.
	public var inserts = IndexSet()

	/// The relative positions of removed elements **prior to** any changes being applied.
	/// These are valid only with the `previous` snapshot.
	///
	/// - important: To obtain the actual index, you must query the `index(_:offsetBy:)`
	///              method on `previous`.
	public var removals = IndexSet()

	/// The relative positions of mutations. These are valid with both the `previous` and
	/// the `current` snapshot.
	///
	/// Mutations imply the same relative position, but the actual indexes could be
	/// different after the changes were applied.
	///
	/// - important: To obtain the actual index, you must query the `index(_:offsetBy:)`
	///              method on either `previous` or `current`, depending on whether the
	///              old value or the new value is the interest.
	public var mutations = IndexSet()

	/// The relative movements. The `previous` offsets are valid with the `previous`
	/// snapshot, and the `current` offsets are valid with the `current` snapshot.
	///
	/// - important: To obtain the actual index, you must query the `index(_:offsetBy:)`
	///              method on either `previous` or `current` as appropriate.
	public var moves = [(previous: Int, current: Int)]()

	public init(current: Elements, previous: Elements? = nil) {
		self.current = current
		self.previous = previous
	}
}

/// A protocol that can be used to constrain associated types as collection deltas.
public protocol CollectionDeltaProtocol {
	associatedtype Elements: Collection

	var event: CollectionDelta<Elements> { get }
}

/// The comparison strategies used by the collection diffing operators on collections
/// that contain `Hashable` objects.
public struct ObjectDiffStrategy {
	fileprivate enum Kind {
		case identity
		case value
	}

	/// Compare the elements by their object identity.
	public static let identity = ObjectDiffStrategy(kind: .identity)

	/// Compare the elements by their value equality.
	public static let value = ObjectDiffStrategy(kind: .value)

	fileprivate let kind: Kind

	private init(kind: Kind) {
		self.kind = kind
	}
}

extension Signal where Value: Collection, Value.Iterator.Element: Equatable, Value.Indices.Iterator.Element == Value.Index {
	/// Compute the difference of `self` with regard to `old` by value equality.
	///
	/// `diff(with:)` works best with collections that contain unique values.
	///
	/// If the elements are repeated per the definition of `Element.==`, `diff(with:)`
	/// cannot guarantee a deterministic stable order, so these would all be uniformly
	/// treated as removals and inserts.
	///
	/// - precondition: The collection type must exhibit array semantics.
	///
	/// - complexity: O(n) time and space.
	public func diff() -> Signal<CollectionDelta<Value>, Error> {
		return diff { $0.diff(with: $1, key: EquatableDiffKey.init, equals: ==) }
	}
}

extension Signal where Value: Collection, Value.Iterator.Element: Hashable, Value.Indices.Iterator.Element == Value.Index {
	/// Compute the difference of `self` with regard to `old` by value equality.
	///
	/// `diff(with:)` works best with collections that contain unique values.
	///
	/// If the elements are repeated per the definition of `Element.==`, `diff(with:)`
	/// cannot guarantee a deterministic stable order, so these would all be uniformly
	/// treated as removals and inserts.
	///
	/// - precondition: The collection type must exhibit array semantics.
	///
	/// - complexity: O(n) time and space.
	public func diff() -> Signal<CollectionDelta<Value>, Error> {
		return diff { $0.diff(with: $1, key: HashableDiffKey.init, equals: ==) }
	}
}

extension Signal where Value: Collection, Value.Iterator.Element: AnyObject, Value.Indices.Iterator.Element == Value.Index {
	/// Compute the difference of `self` with regard to `old` by object identity.
	///
	/// - precondition: The collection type must exhibit array semantics.
	///
	/// - complexity: O(n) time and space.
	public func diff() -> Signal<CollectionDelta<Value>, Error> {
		return diff { $0.diff(with: $1, key: ObjectPointerDiffKey.init, equals: ===) }
	}
}

extension Signal where Value: Collection, Value.Iterator.Element: AnyObject & Equatable, Value.Indices.Iterator.Element == Value.Index {
	/// Compute the difference of `self` with regard to `old` by object identity.
	///
	/// - precondition: The collection type must exhibit array semantics.
	///
	/// - complexity: O(n) time and space.
	public func diff() -> Signal<CollectionDelta<Value>, Error> {
		return diff { $0.diff(with: $1, key: ObjectPointerDiffKey.init, equals: ==) }
	}
}

extension Signal where Value: Collection, Value.Iterator.Element: AnyObject & Hashable, Value.Indices.Iterator.Element == Value.Index {
	/// Compute the difference of `self` with regard to `old` by value equality.
	///
	/// - note: If you wish to compare by object identity, use `diff(.identity)` instead.
	///
	/// - precondition: The collection type must exhibit array semantics.
	///
	/// - complexity: O(n) time and space.
	public func diff() -> Signal<CollectionDelta<Value>, Error> {
		return diff(.value)
	}

	/// Compute the difference of `self` with regard to `old` by the given strategy.
	///
	/// - precondition: The collection type must exhibit array semantics.
	///
	/// - parameters:
	///   - strategy: The comparison strategy to use.
	///
	/// - complexity: O(n) time and space.
	public func diff(_ strategy: ObjectDiffStrategy) -> Signal<CollectionDelta<Value>, Error> {
		switch strategy.kind {
		case .value:
			return diff { $0.diff(with: $1, key: HashableObjectDiffKey.init, equals: ==) }
		case .identity:
			return diff { $0.diff(with: $1, key: ObjectPointerDiffKey.init, equals: ===) }
		}
	}
}

extension Signal where Value: Collection {
	fileprivate func diff(differ: @escaping (_ new: Value, _ old: Value) -> CollectionDelta<Value>) -> Signal<CollectionDelta<Value>, Error> {
		return Signal<CollectionDelta<Value>, Error> { observer in
			var previous: Value?
			return self.observe { event in
				switch event {
				case let .value(elements):
					if let previous = previous {
						observer.send(value: differ(elements, previous))
					} else {
						observer.send(value: CollectionDelta(current: elements))
					}
					previous = elements
				case .completed:
					observer.sendCompleted()
				case let .failed(error):
					observer.send(error: error)
				case .interrupted:
					observer.sendInterrupted()
				}
			}
		}
	}
}

extension SignalProducer where Value: Collection, Value.Iterator.Element: Equatable, Value.Indices.Iterator.Element == Value.Index {
	/// Compute the difference of `self` with regard to `old` by value equality.
	///
	/// `diff(with:)` works best with collections that contain unique values.
	///
	/// If the elements are repeated per the definition of `Element.==`, `diff(with:)`
	/// cannot guarantee a deterministic stable order, so these would all be uniformly
	/// treated as removals and inserts.
	///
	/// - precondition: The collection type must exhibit array semantics.
	///
	/// - complexity: O(n) time and space.
	public func diff() -> SignalProducer<CollectionDelta<Value>, Error> {
		return lift { $0.diff() }
	}
}

extension SignalProducer where Value: Collection, Value.Iterator.Element: Hashable, Value.Indices.Iterator.Element == Value.Index {
	/// Compute the difference of `self` with regard to `old` by value equality.
	///
	/// `diff(with:)` works best with collections that contain unique values.
	///
	/// If the elements are repeated per the definition of `Element.==`, `diff(with:)`
	/// cannot guarantee a deterministic stable order, so these would all be uniformly
	/// treated as removals and inserts.
	///
	/// - precondition: The collection type must exhibit array semantics.	
	///
	/// - complexity: O(n) time and space.
	public func diff() -> SignalProducer<CollectionDelta<Value>, Error> {
		return lift { $0.diff() }
	}
}

extension SignalProducer where Value: Collection, Value.Iterator.Element: AnyObject, Value.Indices.Iterator.Element == Value.Index {
	/// Compute the difference of `self` with regard to `old` by object identity.
	///
	/// - precondition: The collection type must exhibit array semantics.
	///
	/// - complexity: O(n) time and space.
	public func diff() -> SignalProducer<CollectionDelta<Value>, Error> {
		return lift { $0.diff() }
	}
}

extension SignalProducer where Value: Collection, Value.Iterator.Element: AnyObject & Equatable, Value.Indices.Iterator.Element == Value.Index {
	/// Compute the difference of `self` with regard to `old` by object identity.
	///
	/// - precondition: The collection type must exhibit array semantics.
	///
	/// - complexity: O(n) time and space.
	public func diff() -> SignalProducer<CollectionDelta<Value>, Error> {
		return lift { $0.diff() }
	}
}

extension SignalProducer where Value: Collection, Value.Iterator.Element: AnyObject & Hashable, Value.Indices.Iterator.Element == Value.Index {
	/// Compute the difference of `self` with regard to `old` by value equality.
	///
	/// - note: If you wish to compare by object identity, use `diff(.identity)` instead.
	///
	/// - precondition: The collection type must exhibit array semantics.
	///
	/// - complexity: O(n) time and space.
	public func diff() -> SignalProducer<CollectionDelta<Value>, Error> {
		return lift { $0.diff() }
	}

	/// Compute the difference of `self` with regard to `old` by the given strategy.
	///
	/// - precondition: The collection type must exhibit array semantics.
	///
	/// - parameters:
	///   - strategy: The comparison strategy to use.
	///
	/// - complexity: O(n) time and space.
	public func diff(_ strategy: ObjectDiffStrategy) -> SignalProducer<CollectionDelta<Value>, Error> {
		return lift { $0.diff(strategy) }
	}
}

// FIXME: Swift 3.2 compiler inference issue?
#if !swift(>=3.2)
extension PropertyProtocol where Value: Collection, Value.Iterator.Element: Equatable, Value.Indices.Iterator.Element == Value.Index {
	/// Compute the difference of `self` with regard to `old` by value equality.
	///
	/// `diff(with:)` works best with collections that contain unique values.
	///
	/// If the elements are repeated per the definition of `Element.==`, `diff(with:)`
	/// cannot guarantee a deterministic stable order, so these would all be uniformly
	/// treated as removals and inserts.
	///
	/// - precondition: The collection type must exhibit array semantics.
	///
	/// - complexity: O(n) time and space.
	public func diff() -> Property<CollectionDelta<Value>> {
		return lift { $0.diff() }
	}
}

extension PropertyProtocol where Value: Collection, Value.Iterator.Element: Hashable, Value.Indices.Iterator.Element == Value.Index {
	/// Compute the difference of `self` with regard to `old` by value equality.
	///
	/// `diff(with:)` works best with collections that contain unique values.
	///
	/// If the elements are repeated per the definition of `Element.==`, `diff(with:)`
	/// cannot guarantee a deterministic stable order, so these would all be uniformly
	/// treated as removals and inserts.
	///
	/// - precondition: The collection type must exhibit array semantics.
	///
	/// - complexity: O(n) time and space.
	public func diff() -> Property<CollectionDelta<Value>> {
		return lift { $0.diff() }
	}
}

extension PropertyProtocol where Value: Collection, Value.Iterator.Element: AnyObject, Value.Indices.Iterator.Element == Value.Index {
	/// Compute the difference of `self` with regard to `old` by object identity.
	///
	/// - precondition: The collection type must exhibit array semantics.
	///
	/// - complexity: O(n) time and space.
	public func diff() -> Property<CollectionDelta<Value>> {
		return lift { $0.diff() }
	}
}

extension PropertyProtocol where Value: Collection, Value.Iterator.Element: AnyObject & Equatable, Value.Indices.Iterator.Element == Value.Index {
	/// Compute the difference of `self` with regard to `old` by object identity.
	///
	/// - precondition: The collection type must exhibit array semantics.
	///
	/// - complexity: O(n) time and space.
	public func diff() -> Property<CollectionDelta<Value>> {
		return lift { $0.diff() }
	}
}

extension PropertyProtocol where Value: Collection, Value.Iterator.Element: AnyObject & Hashable, Value.Indices.Iterator.Element == Value.Index {
	/// Compute the difference of `self` with regard to `old` by value equality.
	///
	/// - note: If you wish to compare by object identity, use `diff(.identity)` instead.
	///
	/// - precondition: The collection type must exhibit array semantics.
	///
	/// - complexity: O(n) time and space.
	public func diff() -> Property<CollectionDelta<Value>> {
		return lift { $0.diff() }
	}

	/// Compute the difference of `self` with regard to `old` by the given strategy.
	///
	/// - precondition: The collection type must exhibit array semantics.
	///
	/// - parameters:
	///   - strategy: The comparison strategy to use.
	///
	/// - complexity: O(n) time and space.
	public func diff(_ strategy: ObjectDiffStrategy) -> Property<CollectionDelta<Value>> {
		return lift { $0.diff(strategy) }
	}
}
#endif

// MARK: - Implementation details

private final class DiffEntry {
	var occurenceInOld = 0
	var occurenceInNew = 0
	var locationInOld: Int?
}

private enum DiffReference {
	case remote(Int)
	case table(DiffEntry)
}

private final class EquatableDiffKey<Value: Equatable>: Hashable {
	let value: Value
	let hashValue: Int

	init(_ value: Value) {
		self.value = value

		var temp = value
		hashValue = withUnsafeMutableBytes(of: &temp) { bytes in
			// The nasty way to generate a hash from non-hashable types: use the raw
			// bytes.
			return bytes.prefix(MemoryLayout<Int>.size).reduce(Int(0)) { $0 << 8 | Int($1) }
		}
	}

	static func ==(left: EquatableDiffKey<Value>, right: EquatableDiffKey<Value>) -> Bool {
		return left.hashValue == right.hashValue && left.value == right.value
	}
}

private final class HashableDiffKey<Value: Hashable>: Hashable {
	let value: Value

	var hashValue: Int {
		return value.hashValue
	}

	init(_ value: Value) {
		self.value = value
	}

	static func ==(left: HashableDiffKey<Value>, right: HashableDiffKey<Value>) -> Bool {
		return left.value == right.value
	}
}

private struct ObjectPointerDiffKey: Hashable {
	unowned(unsafe) let object: AnyObject

	var hashValue: Int {
		return ObjectIdentifier(object).hashValue
	}

	init(_ object: AnyObject) {
		self.object = object
	}

	static func ==(left: ObjectPointerDiffKey, right: ObjectPointerDiffKey) -> Bool {
		return left.object === right.object
	}
}

private struct HashableObjectDiffKey<Object: AnyObject & Hashable>: Hashable {
	unowned(unsafe) let object: Object

	var hashValue: Int {
		return object.hashValue
	}

	init(_ object: Object) {
		self.object = object
	}

	static func ==(left: HashableObjectDiffKey<Object>, right: HashableObjectDiffKey<Object>) -> Bool {
		return left.object == right.object
	}
}

extension Collection {
	fileprivate func diff<Key: Hashable>(
		with old: Self,
		key: (Iterator.Element) -> Key,
		equals: (Iterator.Element, Iterator.Element) -> Bool
	) -> CollectionDelta<Self> where Indices.Iterator.Element == Index {
		var table: [Key: DiffEntry] = Dictionary(minimumCapacity: Int(self.count))
		var oldReferences: [DiffReference] = []
		var newReferences: [DiffReference] = []

		oldReferences.reserveCapacity(Int(old.count))
		newReferences.reserveCapacity(Int(self.count))

		// Pass 1: Scan the new snapshot.
		for element in self {
			let key = key(element)
			let entry = table[key] ?? {
				let entry = DiffEntry()
				table[key] = entry
				return entry
			}()

			entry.occurenceInNew += 1
			newReferences.append(.table(entry))
		}

		// Pass 2: Scan the old snapshot.
		for (offset, index) in old.indices.enumerated() {
			let key = key(old[index])
			let entry = table[key] ?? {
				let entry = DiffEntry()
				table[key] = entry
				return entry
			}()

			entry.occurenceInOld += 1
			entry.locationInOld = offset
			oldReferences.append(.table(entry))
		}

		// Pass 3: Single-occurence lines
		for newPosition in newReferences.startIndex ..< newReferences.endIndex {
			switch newReferences[newPosition] {
			case let .table(entry):
				if entry.occurenceInNew == 1 && entry.occurenceInNew == entry.occurenceInOld {
					let oldPosition = entry.locationInOld!
					newReferences[newPosition] = .remote(oldPosition)
					oldReferences[oldPosition] = .remote(newPosition)
				}

			case .remote:
				break
			}
		}

		var diff = CollectionDelta<Self>(current: self, previous: old)

		// Final Pass: Compute diff.
		for position in oldReferences.indices {
			switch oldReferences[position] {
			case .table:
				// Deleted
				diff.removals.insert(position)

			case let .remote(newPosition):
				if newPosition == position {
					// Same line
					let oldIndex = old.index(old.startIndex, offsetBy: IndexDistance(position))
					let newIndex = self.index(self.startIndex, offsetBy: IndexDistance(newPosition))

					if !equals(old[oldIndex], self[newIndex]) {
						diff.mutations.insert(position)
					}
				} else {
					diff.moves.append((previous: position, current: newPosition))
				}
			}
		}

		for position in newReferences.indices {
			if case .table = newReferences[position] {
				diff.inserts.insert(position)
			}
		}

		return diff
	}
}

#if !swift(>=3.2)
	extension SignedInteger {
		fileprivate init<I: SignedInteger>(_ integer: I) {
			self.init(integer.toIntMax())
		}
	}
#endif

#if DEBUG
	extension Dictionary {
		fileprivate var prettyDebugDescription: String {
			return (["["] + self.map { "\t\($0.key): \($0.value)" } + ["]"]).joined(separator: "\n")
		}
	}

	extension Array {
		fileprivate var prettyDebugDescription: String {
			return (["["] + self.map { "\t\($0)" } + ["]"]).joined(separator: "\n")
		}
	}

	extension ObjectIdentifier {
		fileprivate var hexString: String {
			return String(hashValue, radix: 16, uppercase: true)
		}
	}

	extension DiffEntry: CustomStringConvertible {
		fileprivate var description: String {
			return "[addr \(ObjectIdentifier(self).hexString), #old \(occurenceInOld), #new \(occurenceInNew), oldLoc \(locationInOld.map(String.init(describing:)) ?? "nil")]"
		}
	}

	extension DiffReference: CustomStringConvertible {
		fileprivate var description: String {
			switch self {
			case let .remote(index):
				return "remote[\(index)]"

			case let .table(entry):
				return "entry[\(ObjectIdentifier(entry).hexString)]"
			}
		}
	}
#endif

