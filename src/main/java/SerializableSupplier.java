import java.io.Serializable;
import java.util.function.Supplier;

//declare an interface with only one method (funcitonal) to allow instances of lamba's functions
@FunctionalInterface
//Supplier<T> is a Java's class and it is serializable
public interface SerializableSupplier<T> extends Supplier<T>, Serializable {
}
