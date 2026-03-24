package com.etl.sfdc.storage.controller;

import com.etl.sfdc.storage.dto.DatabaseConnectionTestResponse;
import com.etl.sfdc.storage.dto.DatabaseStorageDetailResponse;
import com.etl.sfdc.storage.dto.DatabaseStorageListPageResponse;
import com.etl.sfdc.storage.dto.DatabaseStorageRegistrationRequest;
import com.etl.sfdc.storage.dto.DatabaseStorageRegistrationResponse;
import com.etl.sfdc.storage.service.DatabaseConnectionTestService;
import com.etl.sfdc.storage.service.DatabaseStorageQueryService;
import com.etl.sfdc.storage.service.DatabaseStorageRegistrationService;
import com.etlplatform.common.storage.database.DatabaseAuthMethod;
import com.etlplatform.common.storage.database.DatabaseCertificateAuthRequest;
import com.etlplatform.common.storage.database.DatabaseVendor;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@RequiredArgsConstructor
@RequestMapping("/storages/databases")
public class StoragePageController {

    private final DatabaseConnectionTestService connectionTestService;
    private final DatabaseStorageRegistrationService registrationService;
    private final DatabaseStorageQueryService queryService;

    @GetMapping({"", "/new"})
    public String databaseStorageManagementPage(@RequestParam(value = "page", defaultValue = "0") int page,
                                                @RequestParam(value = "size", defaultValue = "10") int size,
                                                @RequestParam(value = "sortBy", defaultValue = "id") String sortBy,
                                                @RequestParam(value = "sortDirection", defaultValue = "asc") String sortDirection,
                                                @RequestParam(value = "selectedStorageId", required = false) Long selectedStorageId,
                                                Model model) {
        preparePage(model, getOrCreateForm(model), page, size, sortBy, sortDirection, selectedStorageId);
        return "database_storage_management";
    }

    @PostMapping("/test")
    public String testConnection(@Valid @ModelAttribute("storageForm") DatabaseStorageRegistrationRequest form,
                                 BindingResult bindingResult,
                                 @RequestParam(value = "page", defaultValue = "0") int page,
                                 @RequestParam(value = "size", defaultValue = "10") int size,
                                 @RequestParam(value = "sortBy", defaultValue = "id") String sortBy,
                                 @RequestParam(value = "sortDirection", defaultValue = "asc") String sortDirection,
                                 @RequestParam(value = "selectedStorageId", required = false) Long selectedStorageId,
                                 Model model) {
        preparePage(model, form, page, size, sortBy, sortDirection, selectedStorageId);

        if (bindingResult.hasErrors()) {
            model.addAttribute("resultTone", "error");
            model.addAttribute("resultTitle", "연결 테스트 실패");
            model.addAttribute("resultMessage", firstErrorMessage(bindingResult));
            model.addAttribute("testPassed", false);
            return "database_storage_management";
        }

        DatabaseConnectionTestResponse response = connectionTestService.test(form);
        model.addAttribute("resultTone", response.isSuccess() ? "success" : "warning");
        model.addAttribute("resultTitle", response.isSuccess() ? "연결 테스트 성공" : "연결 테스트 결과 확인 필요");
        model.addAttribute("resultMessage", response.getMessage() == null || response.getMessage().isBlank()
                ? "연결 테스트가 끝났어요."
                : response.getMessage());
        model.addAttribute("testPassed", response.isSuccess());
        return "database_storage_management";
    }

    @PostMapping
    public String register(@Valid @ModelAttribute("storageForm") DatabaseStorageRegistrationRequest form,
                           BindingResult bindingResult,
                           @RequestParam(value = "page", defaultValue = "0") int page,
                           @RequestParam(value = "size", defaultValue = "10") int size,
                           @RequestParam(value = "sortBy", defaultValue = "id") String sortBy,
                           @RequestParam(value = "sortDirection", defaultValue = "asc") String sortDirection,
                           @RequestParam(value = "selectedStorageId", required = false) Long selectedStorageId,
                           Model model) {
        preparePage(model, form, page, size, sortBy, sortDirection, selectedStorageId);

        if (bindingResult.hasErrors()) {
            model.addAttribute("resultTone", "error");
            model.addAttribute("resultTitle", "저장소 등록 실패");
            model.addAttribute("resultMessage", firstErrorMessage(bindingResult));
            model.addAttribute("testPassed", false);
            return "database_storage_management";
        }

        DatabaseStorageRegistrationResponse response = registrationService.register(form);
        DatabaseStorageRegistrationRequest emptyForm = emptyForm();
        preparePage(model, emptyForm, page, size, sortBy, sortDirection, response.getId());
        model.addAttribute("resultTone", "success");
        model.addAttribute("resultTitle", "저장소 등록 완료");
        model.addAttribute("resultMessage", String.format("%s 저장소를 %s / %s 설정으로 등록했어요. 이제 아래 목록에서 상세 정보와 복호화 흐름까지 확인할 수 있어요.",
                response.getName(), response.getVendor(), response.getAuthMethod()));
        model.addAttribute("testPassed", false);
        return "database_storage_management";
    }

    private void preparePage(Model model,
                             DatabaseStorageRegistrationRequest form,
                             int page,
                             int size,
                             String sortBy,
                             String sortDirection,
                             Long selectedStorageId) {
        DatabaseStorageListPageResponse listPage = queryService.getList(page, size, sortBy, sortDirection);
        DatabaseStorageDetailResponse selectedDetail = resolveSelectedDetail(listPage, selectedStorageId);

        model.addAttribute("vendors", DatabaseVendor.values());
        model.addAttribute("authMethods", DatabaseAuthMethod.values());
        model.addAttribute("storageForm", form);
        model.addAttribute("testPassed", Boolean.TRUE.equals(model.getAttribute("testPassed")));
        model.addAttribute("listPage", listPage);
        model.addAttribute("selectedDetail", selectedDetail);
        model.addAttribute("selectedStorageId", selectedDetail != null ? selectedDetail.getId() : null);
        model.addAttribute("page", listPage.getPage());
        model.addAttribute("size", listPage.getSize());
        model.addAttribute("sortBy", sortBy);
        model.addAttribute("sortDirection", sortDirection);
        model.addAttribute("hasPrevPage", listPage.getPage() > 0);
        model.addAttribute("hasNextPage", listPage.getPage() + 1 < listPage.getTotalPages());
        model.addAttribute("startRow", listPage.getTotalElements() == 0 ? 0 : (listPage.getPage() * listPage.getSize()) + 1);
        model.addAttribute("endRow", Math.min((listPage.getPage() + 1) * listPage.getSize(), listPage.getTotalElements()));
    }

    private DatabaseStorageDetailResponse resolveSelectedDetail(DatabaseStorageListPageResponse listPage, Long selectedStorageId) {
        if (selectedStorageId != null) {
            return queryService.getDetail(selectedStorageId);
        }
        if (listPage.getItems() == null || listPage.getItems().isEmpty()) {
            return null;
        }
        return queryService.getDetail(listPage.getItems().get(0).getId());
    }

    private DatabaseStorageRegistrationRequest getOrCreateForm(Model model) {
        Object existing = model.getAttribute("storageForm");
        if (existing instanceof DatabaseStorageRegistrationRequest request) {
            ensureNested(request);
            return request;
        }
        return emptyForm();
    }

    private DatabaseStorageRegistrationRequest emptyForm() {
        DatabaseStorageRegistrationRequest request = new DatabaseStorageRegistrationRequest();
        request.setAuthMethod(DatabaseAuthMethod.PASSWORD);
        ensureNested(request);
        return request;
    }

    private void ensureNested(DatabaseStorageRegistrationRequest request) {
        if (request.getCertificateAuth() == null) {
            request.setCertificateAuth(new DatabaseCertificateAuthRequest());
        }
    }

    private String firstErrorMessage(BindingResult bindingResult) {
        if (bindingResult.getFieldError() != null) {
            return bindingResult.getFieldError().getDefaultMessage();
        }
        return "입력값을 다시 확인해 주세요.";
    }
}
