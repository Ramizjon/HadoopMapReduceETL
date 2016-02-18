package com.segmentreader.mapreduce;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.segmentreader.dataformats.Convertor;
import com.segmentreader.dataformats.ConvertorImpl;
import com.segmentreader.domain.HBaseUserRepositoryImpl;
import com.segmentreader.domain.UserRepository;
import com.segmentreader.useroperations.AddOperationHandler;
import com.segmentreader.useroperations.DeleteOperationHandler;
import com.segmentreader.useroperations.OperationHandler;
import com.segmentreader.utils.ParquetAppender;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;


/**
 * Contains all necessary classes, being a connecting link of application
 * @author Ramizjon
 *
 */
public class AppContext {

    /**
     * Instance to access a user repository
     */
    private static HBaseUserRepositoryImpl userRepository = null;
    
    private static HBaseUserRepositoryImpl getUserRepoInstance() {
        if (userRepository == null){
                try {
                    userRepository = new HBaseUserRepositoryImpl();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
        }
        return userRepository;
    }

    public static class CookieReducer extends AbstractCookieReducer {

        @Override
        protected Map<String, OperationHandler> getHandlers() {
            Map<String, OperationHandler> handlersMap = new HashMap<String, OperationHandler>();
            handlersMap.put(OperationHandler.ADD_OPERATION,
                    new AppContext.AddOperationHandlerImpl());
            handlersMap.put(OperationHandler.DELETE_OPERATION,
                    new AppContext.DeleteOperationHandlerImpl());
            return handlersMap;
        }

        @Override
        protected ParquetAppender<ParquetCompatibleUserModCommand> getParquetAppender(Context context) {
            return new ParquetAppender<ParquetCompatibleUserModCommand>(FileOutputFormat.getOutputPath(
                    new JobConf(context.getConfiguration())).toString(), ParquetCompatibleUserModCommand.class);
        }
    }

    /**
     * implementation of main application mapper class
     * 
     * @author Ramizjon
     *
     */
    public static class UserSegmentsMapper extends AbstractUserSegmentsMapper {
        
        @Override
        protected List<Closeable> getCloseables() {
            return Arrays.<Closeable> asList(getUserRepoInstance());
        }

        @Override
        protected Convertor getConvertor() {
            return new ConvertorImpl();
        }
    }

    /**
     * Implementation of AddOperationHandler, that has direct access to repository instance
     * @author Ramizjon
     *
     */
    public static class AddOperationHandlerImpl extends AddOperationHandler {

        @Override
        protected UserRepository getRepoInstance() {
            return getUserRepoInstance();
        }
    }

    /**
     * Implementation of DeleteOperationHandler, that has direct access to repository instance
     * @author Ramizjon
     *
     */
    public static class DeleteOperationHandlerImpl extends
            DeleteOperationHandler {

        @Override
        protected UserRepository getRepoInstance() {
            return getUserRepoInstance();
        }

    }
}
