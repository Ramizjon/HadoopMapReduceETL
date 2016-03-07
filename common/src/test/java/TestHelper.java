import java.util.ArrayList;
import java.util.List;

public class TestHelper<T> {

    public ArrayList<T> toArrayList (List<T> inputList){
        return new ArrayList<T>(inputList);
    }
}
