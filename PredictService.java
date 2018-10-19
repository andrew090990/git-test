package ru.skoltech.aeronetlab.moz.services.inference;


import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.stereotype.Service;
import ru.skoltech.aeronetlab.moz.dal.ChangeDetectionFileSystemStorage;
import ru.skoltech.aeronetlab.moz.dal.TimeFrameRepository;
import ru.skoltech.aeronetlab.moz.dal.TimeSeriesRepository;
import ru.skoltech.aeronetlab.moz.exceptions.http.BadRequest;
import ru.skoltech.aeronetlab.moz.exceptions.http.HttpException;
import ru.skoltech.aeronetlab.moz.exceptions.http.InternalServerError;
import ru.skolt
import java.util.stream.Collectors;

@Service
public class PredictService {

    private static final Logger log = LoggerFactory.getLogger(PredictService.class);

    private String tmpFilesDirectory;

    private static final Object locker = new Object();

    private GeoserverPublisher geoserverPublisher;
    private TimeSeriesRepository timeSeriesRepository;
    private TimeFrameRepository timeFrameRepository;
    private ChangeDetectionFileSystemStorage storage;
    private ZipService zipService;
    private InferenceService inferenceService;

    public PredictService(GeoserverPublisher geoserverPublisher,
                             TimeSeriesRepository timeSeriesRepository,
                             TimeFrameRepository timeFrameRepository,
                             ChangeDetectionFileSystemStorage storage,
                             ZipService zipService,
                             InferenceService inferenceService,
                             @Value("${tmp.files:/backend-data/tmp}")
                                  String tmpFilesDirectory) {

        this.geoserverPublisher = geoserverPublisher;
        this.timeSeriesRepository = timeSeriesRepository;
        this.timeFrameRepository = timeFrameRepository;
        this.storage = storage;
        this.zipService = zipService;
        this.tmpFilesDirectory = tmpFilesDirectory;
        this.inferenceService = inferenceService;
    }

    public void predict(String datasetId, String timeSeriesId) throws HttpException {

        if (!timeSeriesRepository.containsTimeSeries(datasetId, timeSeriesId)) {
            log.error(String.format("Can't find time series %s %s", datasetId, timeSeriesId));
            throw new TimeSeriesNotFound(timeSeriesId);
        }

        List<Path> timeFramePaths = getTimeFramePaths(datasetId, timeSeriesId);

        //Support only pairs so far
        if(timeFramePaths.size() != 2) {
            log.error("TimeSeries {}  is not a pair", timeSeriesId);
            throw new BadRequest(String.format("TimeSeries %s is not a pair", timeSeriesId));
        }


        synchronized (locker) {

            String zipFile = zipService.zipTimeframesChannels(timeFramePaths, tmpFilesDirectory);
            FileSystemResource zippedTimeFrames = new FileSystemResource(zipFile);

            ByteArrayResource zip = inferenceService.predict(zippedTimeFrames);

            log.info("Zip: " + zip);

            List<File> tifs = getTifs(zip);
            geoserverPublisher.uploadRasterPredictions(tifs);
        }
    }

    private List<Path> getTimeFramePaths(String datasetId, String timeSeriesId) {

        return timeFrameRepository.getTimeFramesForTimeSeries(datasetId, timeSeriesId).stream()
                .map(timeframe -> storage.getPath(datasetId, timeSeriesId, timeframe.getId()))
                .map(path -> Paths.get(path))
                .collect(Collectors.toList());
    }

    private List<File> getTifs(ByteArrayResource zip) throws InternalServerError {

        try {
            List<File> files = zipService.unzipFile(zip);
            log.info("Files: " + files);

            return files.stream()
                    .filter(file -> FilenameUtils.getExtension(file.getName()).equals("tif"))
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new InternalServerError("Error during unzipping predictions");
        }
    }
}
