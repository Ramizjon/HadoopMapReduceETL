package com.segmentreader.mapreduce;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.segmentreader.domain.HBaseUserRepositoryImpl;
import com.segmentreader.useroperations.AddOperationHandler;
import com.segmentreader.useroperations.DeleteOperationHandler;
import com.segmentreader.useroperations.OperationHandler;


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

    /**
     * implementation of main application mapper class
     * 
     * @author Ramizjon
     *
     */
    public static class UserSegmentsMapper extends AbstractUserSegmentsMapper {
        protected Map<String, OperationHandler> getHandlers() {
            Map<String, OperationHandler> handlersMap = new HashMap<String, OperationHandler>();
            handlersMap.put(OperationHandler.ADD_OPERATION,
                    new AppContext.AddOperationHandlerImpl());
            handlersMap.put(OperationHandler.DELETE_OPERATION,
                    new AppContext.DeleteOperationHandlerImpl());
            return handlersMap;
        }

        @Override
        protected List<Closeable> getCloseables() {
            return Arrays.<Closeable> asList(getUserRepoInstance());
        }
    }

    /**
     * Implementation of AddOperationHandler, that has direct access to repository instance
     * @author Ramizjon
     *
     */
    public static class AddOperationHandlerImpl extends AddOperationHandler {

        @Override
        protected HBaseUserRepositoryImpl getRepoInstance() {
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
        protected HBaseUserRepositoryImpl getRepoInstance() {
            return getUserRepoInstance();
        }

    }
}
