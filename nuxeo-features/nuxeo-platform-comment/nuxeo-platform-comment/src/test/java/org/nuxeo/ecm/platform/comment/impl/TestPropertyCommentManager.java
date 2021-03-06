/*
 * (C) Copyright 2018 Nuxeo (http://nuxeo.com/) and others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Funsho David
 *     Nuno Cunha <ncunha@nuxeo.com>
 */

package org.nuxeo.ecm.platform.comment.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.nuxeo.ecm.platform.comment.api.Comments.commentToDocumentModel;
import static org.nuxeo.ecm.platform.comment.api.Comments.newComment;
import static org.nuxeo.ecm.platform.comment.workflow.utils.CommentsConstants.COMMENT_DOC_TYPE;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.ListIterator;

import org.junit.Test;
import org.nuxeo.ecm.core.api.CloseableCoreSession;
import org.nuxeo.ecm.core.api.CoreInstance;
import org.nuxeo.ecm.core.api.CoreSession;
import org.nuxeo.ecm.core.api.DocumentModel;
import org.nuxeo.ecm.core.api.DocumentModelList;
import org.nuxeo.ecm.core.api.DocumentSecurityException;
import org.nuxeo.ecm.core.api.IdRef;
import org.nuxeo.ecm.core.api.PathRef;
import org.nuxeo.ecm.core.api.security.ACE;
import org.nuxeo.ecm.core.api.security.ACL;
import org.nuxeo.ecm.core.api.security.SecurityConstants;
import org.nuxeo.ecm.core.api.security.impl.ACPImpl;
import org.nuxeo.ecm.platform.comment.AbstractTestCommentManager;
import org.nuxeo.ecm.platform.comment.api.Comment;
import org.nuxeo.ecm.platform.comment.api.CommentImpl;
import org.nuxeo.ecm.platform.comment.api.exceptions.CommentNotFoundException;
import org.nuxeo.runtime.test.runner.Deploy;

/**
 * @since 10.3
 */
@Deploy("org.nuxeo.ecm.platform.query.api")
public class TestPropertyCommentManager extends AbstractTestCommentManager {

    @Test
    public void shouldThrowExceptionWhenGettingNonExistingComment() {
        try {
            commentManager.getComment(session, "nonExistingCommentId");
            fail("This test is expected to fail!");
        } catch (CommentNotFoundException e) {
            assertEquals(404, e.getStatusCode());
            assertEquals("The comment nonExistingCommentId does not exist.", e.getMessage());
        }

    }

    @Test
    public void shouldThrowExceptionWhenGettingNonExistingExternalComment() {
        try {
            commentManager.getExternalComment(session, "nonExistingExternalCommentId");
            fail("This test is expected to fail!");
        } catch (CommentNotFoundException e) {
            assertEquals(404, e.getStatusCode());
            assertEquals("The external comment nonExistingExternalCommentId does not exist.", e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionWhenCreatingCommentForNonExistingParent() {
        try {
            commentManager.createComment(session,
                    createSampleComment("nonExistingId", session.getPrincipal().getName(), "some text"));
            fail("This test is expected to fail!");
        } catch (CommentNotFoundException e) {
            assertEquals(404, e.getStatusCode());
            assertEquals("The document or comment nonExistingId does not exist.", e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionWhenUpdatingNonExistingComment() {
        try {
            commentManager.updateComment(session, "nonExistingCommentId", new CommentImpl());
            fail("This test is expected to fail!");
        } catch (CommentNotFoundException e) {
            assertEquals(404, e.getStatusCode());
            assertEquals("The comment nonExistingCommentId does not exist.", e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionWhenUpdatingNonExistingExternalComment() {
        try {
            commentManager.updateExternalComment(session, "nonExistingExternalCommentId", new CommentImpl());
            fail("This test is expected to fail!");
        } catch (CommentNotFoundException e) {
            assertEquals(404, e.getStatusCode());
            assertEquals("The external comment nonExistingExternalCommentId does not exist.", e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionWhenDeletingNonExistingComment() {
        try {
            commentManager.deleteComment(session, "nonExistingCommentId");
            fail("This test is expected to fail!");
        } catch (CommentNotFoundException e) {
            assertEquals(404, e.getStatusCode());
            assertEquals("The comment nonExistingCommentId does not exist.", e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionWhenDeletingNonExistingExternalComment() {
        try {
            commentManager.deleteExternalComment(session, "nonExistingExternalCommentId");
            fail("This test is expected to fail!");
        } catch (CommentNotFoundException e) {
            assertEquals(404, e.getStatusCode());
            assertEquals("The external comment nonExistingExternalCommentId does not exist.", e.getMessage());
        }
    }

    @Test
    public void shouldReturnCreatedObjectWhenCreatingComment() {
        DocumentModel doc = session.createDocumentModel(FOLDER_COMMENT_CONTAINER, "myFile", "File");
        doc = session.createDocument(doc);
        session.save();

        Comment commentToCreate = createSampleComment(doc.getId(), session.getPrincipal().getName(), "some text");

        Comment createdComment = commentManager.createComment(session, commentToCreate);
        assertNotNull(createdComment);
        assertNotNull(createdComment.getId());
        assertEquals(commentToCreate.getCreationDate(), createdComment.getCreationDate());
        assertNotNull(createdComment.getAncestorIds());
        assertEquals(1, createdComment.getAncestorIds().size());
        assertTrue(createdComment.getAncestorIds().contains(doc.getId()));
        assertEquals(commentToCreate.getAuthor(), createdComment.getAuthor());
        assertEquals(doc.getId(), createdComment.getParentId());
        assertEquals(commentToCreate.getText(), createdComment.getText());
        assertNull(createdComment.getModificationDate());
    }

    @Test
    public void shouldReturnCreatedObjectWithCreationDateWhenCreatingCommentWithoutCreationDate() {
        DocumentModel doc = session.createDocumentModel(FOLDER_COMMENT_CONTAINER, "myFile", "File");
        doc = session.createDocument(doc);
        session.save();

        Comment commentToCreate = createSampleComment(doc.getId(), session.getPrincipal().getName(), "some text");
        commentToCreate.setCreationDate(null);

        Comment createdComment = commentManager.createComment(session, commentToCreate);
        assertNotNull(createdComment);
        assertNotNull(createdComment.getId());
        assertNotNull(createdComment.getCreationDate());
        assertNotNull(createdComment.getAncestorIds());
        assertEquals(1, createdComment.getAncestorIds().size());
        assertTrue(createdComment.getAncestorIds().contains(doc.getId()));
        assertEquals(commentToCreate.getAuthor(), createdComment.getAuthor());
        assertEquals(doc.getId(), createdComment.getParentId());
        assertEquals(commentToCreate.getText(), createdComment.getText());
        assertNull(createdComment.getModificationDate());
    }

    @Test
    public void shouldReturnObjectWhenGettingExistingComment() {
        DocumentModel doc = session.createDocumentModel(FOLDER_COMMENT_CONTAINER, "myFile", "File");
        doc = session.createDocument(doc);
        session.save();

        Comment comment = commentManager.createComment(session,
                createSampleComment(doc.getId(), session.getPrincipal().getName(), "some text"));

        Comment storedComment = newComment(session.getDocument(new IdRef(comment.getId())));
        assertNotNull(storedComment);
        assertEquals(comment.getText(), storedComment.getText());
    }

    @Test
    public void shouldReturnObjectWhenGettingExistingExternalComment() {
        DocumentModel doc = session.createDocumentModel(FOLDER_COMMENT_CONTAINER, "myFile", "File");
        doc = session.createDocument(doc);

        Comment comment = createSampleComment(doc.getId(), session.getPrincipal().getName(), "some text");
        ((CommentImpl) comment).setEntityId("anEntityId");
        ((CommentImpl) comment).setEntity("anEntityByItself");
        ((CommentImpl) comment).setOrigin("anOriginForExternalEntity");

        comment = commentManager.createComment(session, comment);

        session.save();

        Comment storedExternalComment = commentManager.getExternalComment(session, "anEntityId");
        assertNotNull(storedExternalComment);
        assertEquals(comment.getText(), storedExternalComment.getText());
    }

    @Test
    public void shouldReflectUpdatedFieldsWhenUpdatingExistingComment() {
        DocumentModel doc = session.createDocumentModel(FOLDER_COMMENT_CONTAINER, "myFile", "File");
        doc = session.createDocument(doc);

        Comment comment = commentManager.createComment(session,
                createSampleComment(doc.getId(), session.getPrincipal().getName(), "some text"));

        session.save();

        comment.setText("my updated text!");
        comment.setModificationDate(Instant.now());
        commentManager.updateComment(session, comment.getId(), comment);

        Comment storedComment = newComment(session.getDocument(new IdRef(comment.getId())));
        assertEquals(comment.getText(), storedComment.getText());
    }

    @Test
    public void shouldReflectUpdatedFieldsWhenUpdatingExistingCommentWithoutProvidingModificationDate() {
        DocumentModel doc = session.createDocumentModel(FOLDER_COMMENT_CONTAINER, "myFile", "File");
        doc = session.createDocument(doc);

        Comment comment = commentManager.createComment(session,
                createSampleComment(doc.getId(), session.getPrincipal().getName(), "some text"));

        session.save();

        comment.setText("my updated text!");
        commentManager.updateComment(session, comment.getId(), comment);

        Comment storedComment = newComment(session.getDocument(new IdRef(comment.getId())));
        assertEquals(comment.getId(), storedComment.getId());
        assertEquals(comment.getAuthor(), storedComment.getAuthor());
        assertEquals(comment.getCreationDate(), storedComment.getCreationDate());
        assertEquals(comment.getText(), storedComment.getText());
        assertEquals(comment.getParentId(), storedComment.getParentId());
        assertEquals(comment.getAncestorIds(), storedComment.getAncestorIds());
        assertNotNull(storedComment.getModificationDate());
    }

    @Test
    public void shouldReflectUpdatedFieldsWhenUpdatingExistingExternalComment() {
        DocumentModel doc = session.createDocumentModel(FOLDER_COMMENT_CONTAINER, "myFile", "File");
        doc = session.createDocument(doc);

        Comment comment = createSampleComment(doc.getId(), session.getPrincipal().getName(), "some text");
        ((CommentImpl) comment).setEntityId("anEntityId");
        ((CommentImpl) comment).setEntity("anEntityByItself");
        ((CommentImpl) comment).setOrigin("anOriginForExternalEntity");

        comment = commentManager.createComment(session, comment);

        session.save();

        comment.setText("my updated text!");
        comment.setModificationDate(Instant.now());
        Comment storedComment = commentManager.updateExternalComment(session, "anEntityId", comment);
        assertEquals(comment.getText(), storedComment.getText());
    }

    @Test
    public void shouldNotBeAvailableWhenExistingCommentIsDeleted() {
        DocumentModel doc = session.createDocumentModel(FOLDER_COMMENT_CONTAINER, "myFile", "File");
        doc = session.createDocument(doc);

        Comment comment = commentManager.createComment(session,
                createSampleComment(doc.getId(), session.getPrincipal().getName(), "some text"));

        session.save();

        commentManager.deleteComment(session, comment.getId());
        assertFalse(session.exists(new IdRef(comment.getId())));
    }

    @Test
    public void shouldNotBeAvailableWhenExistingExternalCommentIsDeleted() {
        DocumentModel doc = session.createDocumentModel(FOLDER_COMMENT_CONTAINER, "myFile", "File");
        doc = session.createDocument(doc);

        Comment comment = createSampleComment(doc.getId(), session.getPrincipal().getName(), "some text");
        ((CommentImpl) comment).setEntityId("anEntityId");
        ((CommentImpl) comment).setEntity("anEntityByItself");
        ((CommentImpl) comment).setOrigin("anOriginForExternalEntity");

        comment = commentManager.createComment(session, comment);

        session.save();

        commentManager.deleteExternalComment(session, "anEntityId");
        assertFalse(session.exists(new IdRef(comment.getId())));
    }

    @Test
    public void shouldReturnEmptyListWhenDocumentHasNoComments() {
        DocumentModel doc = session.createDocumentModel(FOLDER_COMMENT_CONTAINER, "myFile", "File");
        doc = session.createDocument(doc);
        session.save();

        List<DocumentModel> comments = commentManager.getComments(session, doc);
        assertNotNull(comments);
        assertEquals(0, comments.size());
    }

    @Test
    public void shouldReturnAllCommentsHasDocumentModelsSortedByCreationDateAscendingWhenDocumentHasComments() {
        DocumentModel doc = session.createDocumentModel(FOLDER_COMMENT_CONTAINER, "myFile", "File");
        doc = session.createDocument(doc);

        List<Comment> sampleComments = createSampleComments(4, doc.getId(), session.getPrincipal().getName(),
                "comment");

        Comment firstComment = commentManager.createComment(session, sampleComments.get(0));
        Comment secondComment = commentManager.createComment(session, sampleComments.get(1));
        Comment thirdComment = commentManager.createComment(session, sampleComments.get(2));
        Comment fourthComment = commentManager.createComment(session, sampleComments.get(3));

        session.save();

        List<DocumentModel> comments = commentManager.getComments(session, doc);
        assertNotNull(comments);
        assertEquals(4, comments.size());
        assertEquals(newComment(comments.get(0)).getText(), firstComment.getText());
        assertEquals(newComment(comments.get(1)).getText(), secondComment.getText());
        assertEquals(newComment(comments.get(2)).getText(), thirdComment.getText());
        assertEquals(newComment(comments.get(3)).getText(), fourthComment.getText());
    }

    @Test
    public void shouldReturnAllCommentsSortedByCreationDateDescendingWhenDocumentHasComments() {
        DocumentModel doc = session.createDocumentModel(FOLDER_COMMENT_CONTAINER, "myFile", "File");
        doc = session.createDocument(doc);

        List<Comment> sampleComments = createSampleComments(4, doc.getId(), session.getPrincipal().getName(),
                "comment");

        Comment firstComment = commentManager.createComment(session, sampleComments.get(0));
        Comment secondComment = commentManager.createComment(session, sampleComments.get(1));
        Comment thirdComment = commentManager.createComment(session, sampleComments.get(2));
        Comment fourthComment = commentManager.createComment(session, sampleComments.get(3));

        session.save();

        List<Comment> comments = commentManager.getComments(session, doc.getId(), false);
        assertNotNull(comments);
        assertEquals(4, comments.size());
        assertEquals(comments.get(0).getText(), fourthComment.getText());
        assertEquals(comments.get(1).getText(), thirdComment.getText());
        assertEquals(comments.get(2).getText(), secondComment.getText());
        assertEquals(comments.get(3).getText(), firstComment.getText());
    }

    @Test
    public void shouldReturnCommentsPaginatedAndSortedByCreationDateDescendingWhenDocumentHasComments() {
        DocumentModel doc = session.createDocumentModel(FOLDER_COMMENT_CONTAINER, "myFile", "File");
        doc = session.createDocument(doc);

        List<Comment> sampleComments = createSampleComments(4, doc.getId(), session.getPrincipal().getName(),
                "comment");

        Comment firstComment = commentManager.createComment(session, sampleComments.get(0));
        Comment secondComment = commentManager.createComment(session, sampleComments.get(1));
        Comment thirdComment = commentManager.createComment(session, sampleComments.get(2));
        Comment fourthComment = commentManager.createComment(session, sampleComments.get(3));

        session.save();

        List<Comment> firstPage = commentManager.getComments(session, doc.getId(), 2L, 0L, false);
        assertNotNull(firstPage);
        assertEquals(2, firstPage.size());
        assertEquals(firstPage.get(0).getText(), fourthComment.getText());
        assertEquals(firstPage.get(1).getText(), thirdComment.getText());

        List<Comment> secondPage = commentManager.getComments(session, doc.getId(), 2L, 1L, false);
        assertNotNull(secondPage);
        assertEquals(2, secondPage.size());
        assertEquals(secondPage.get(0).getText(), secondComment.getText());
        assertEquals(secondPage.get(1).getText(), firstComment.getText());
    }

    @Test
    public void shouldReturnRepliesByCreationDateDescendingWhenCommentHasReplies() {
        DocumentModel doc = session.createDocumentModel(FOLDER_COMMENT_CONTAINER, "myFile", "File");
        doc = session.createDocument(doc);

        Comment mainComment = commentManager.createComment(session,
                createSampleComment(doc.getId(), session.getPrincipal().getName(), "main comment"));

        List<Comment> sampleReplies = createSampleComments(2, mainComment.getId(), session.getPrincipal().getName(),
                "reply");
        Comment firstReply = commentManager.createComment(session, sampleReplies.get(0));
        Comment secondReply = commentManager.createComment(session, sampleReplies.get(1));

        session.save();

        List<Comment> replies = commentManager.getComments(session, mainComment.getId(), false);
        assertNotNull(replies);
        assertEquals(2, replies.size());
        assertEquals(replies.get(0).getText(), secondReply.getText());
        assertEquals(replies.get(1).getText(), firstReply.getText());
    }

    @Test
    public void shouldReturnMainCommentWhenSeveralNestedRepliesExist() {
        DocumentModel doc = session.createDocumentModel(FOLDER_COMMENT_CONTAINER, "myFile", "File");
        doc = session.createDocument(doc);

        Comment comment = commentManager.createComment(session,
                createSampleComment(doc.getId(), session.getPrincipal().getName(), "main comment"));
        Comment firstLevelReply = commentManager.createComment(session,
                createSampleComment(comment.getId(), session.getPrincipal().getName(), "first level reply"));
        Comment secondLevelReply = commentManager.createComment(session,
                createSampleComment(firstLevelReply.getId(), session.getPrincipal().getName(), "second level reply"));
        Comment thirdLevelReply = commentManager.createComment(session,
                createSampleComment(secondLevelReply.getId(), session.getPrincipal().getName(), "third level reply"));
        Comment fourthLevelReply = commentManager.createComment(session,
                createSampleComment(thirdLevelReply.getId(), session.getPrincipal().getName(), "fourth level reply"));

        session.save();

        DocumentModel replyModel = session.createDocumentModel(FOLDER_COMMENT_CONTAINER, "Comment", COMMENT_DOC_TYPE);
        commentToDocumentModel(fourthLevelReply, replyModel);
        DocumentModel threadDocumentModel = commentManager.getThreadForComment(replyModel);
        assertNotNull(threadDocumentModel);
        assertEquals(newComment(threadDocumentModel).getText(), comment.getText());
    }

    @Test
    public void shouldReturnSameCommentWhenNoRepliesExist() {
        DocumentModel doc = session.createDocumentModel(FOLDER_COMMENT_CONTAINER, "myFile", "File");
        doc = session.createDocument(doc);

        Comment comment = commentManager.createComment(session,
                createSampleComment(doc.getId(), session.getPrincipal().getName(), "main comment"));

        session.save();

        DocumentModel threadDocumentModel = commentManager.getThreadForComment(
                session.getDocument(new IdRef(comment.getId())));

        assertNotNull(threadDocumentModel);
        assertEquals(comment.getText(), newComment(threadDocumentModel).getText());
    }

    @Test
    public void shouldCreateLocatedComment() {
        DocumentModel doc = session.createDocumentModel(FOLDER_COMMENT_CONTAINER, "myFile", "File");
        doc = session.createDocument(doc);

        Comment comment = createSampleComment(null, session.getPrincipal().getName(), "some text");
        DocumentModel commentModel = session.createDocumentModel(null, "Comment", COMMENT_DOC_TYPE);
        commentToDocumentModel(comment, commentModel);

        session.save();

        commentManager.createLocatedComment(doc, commentModel, FOLDER_COMMENT_CONTAINER);

        DocumentModelList children = session.getChildren(new PathRef(FOLDER_COMMENT_CONTAINER), COMMENT_DOC_TYPE);
        assertNotNull(children);
        assertEquals(1, children.totalSize());
        assertEquals(comment.getAuthor(), children.get(0).getPropertyValue("comment:author"));
        assertEquals(comment.getCreationDate(),
                ((Calendar) children.get(0).getPropertyValue("comment:creationDate")).toInstant());
        assertEquals(comment.getText(), children.get(0).getPropertyValue("comment:text"));
    }

    @Test
    public void testAdministratorCanManageComments() {
        DocumentModel doc = createTestFileAndUser("bob");

        Comment comment = createSampleComment(doc.getId(), session.getPrincipal().getName(), "test");
        comment = commentManager.createComment(session, comment);
        session.save();

        testManageComments(session, comment.getId());

        try (CloseableCoreSession bobSession = CoreInstance.openCoreSession(session.getRepositoryName(), "bob")) {
            comment = createSampleComment(doc.getId(), "bob", "test bob");
            comment = commentManager.createComment(bobSession, comment);
            bobSession.save();
        }

        testManageComments(session, comment.getId());
    }

    @Test
    public void testAuthorCanManageComments() {
        DocumentModel doc = createTestFileAndUser("bob");

        try (CloseableCoreSession bobSession = CoreInstance.openCoreSession(session.getRepositoryName(), "bob")) {
            Comment comment = createSampleComment(doc.getId(), "bob", "test");
            comment = commentManager.createComment(session, comment);
            bobSession.save();

            testManageComments(bobSession, comment.getId());
        }
    }

    @Test
    public void testRegularUserCannotManageComments() {
        DocumentModel doc = createTestFileAndUser("bob");

        Comment comment = createSampleComment(doc.getId(), session.getPrincipal().getName(), "test");
        comment = commentManager.createComment(session, comment);
        session.save();

        try (CloseableCoreSession bobSession = CoreInstance.openCoreSession(session.getRepositoryName(), "bob")) {
            testManageComments(bobSession, comment.getId());
            fail("bob should not be able to manage comments created by Administrator");
        } catch (DocumentSecurityException e) {
            // ok
        }
    }

    protected DocumentModel createTestFileAndUser(String user) {
        DocumentModel domain = session.createDocumentModel("/", "domain", "Domain");
        domain = session.createDocument(domain);
        ACPImpl acp = new ACPImpl();
        ACL acl = acp.getOrCreateACL();
        acl.add(new ACE(user, SecurityConstants.READ, true));
        acl.add(new ACE(user, SecurityConstants.ADD_CHILDREN, true));
        acl.add(new ACE(user, SecurityConstants.REMOVE_CHILDREN, true));
        session.setACP(domain.getRef(), acp, false);
        DocumentModel doc = session.createDocumentModel("/domain", "test", "File");
        doc = session.createDocument(doc);
        session.save();

        return doc;
    }


    protected void testManageComments(CoreSession session, String commentId) {
        //Read
        Comment comment = commentManager.getComment(session, commentId);

        // Update
        comment.setText("update");
        commentManager.updateComment(session, comment.getId(), comment);

        // Delete
        commentManager.deleteComment(session, commentId);
    }

    protected Comment createSampleComment(String parentId, String author, String text) {
        return createSampleComments(1, parentId,author,text).get(0);
    }

    protected List<Comment> createSampleComments(int nbComments, String parentId, String author, String text) {
        List<Comment> comments = new ArrayList<>();
        Instant date = Instant.now();
        for (int i = 0; i < nbComments; i++) {
            Comment comment = new CommentImpl();
            comment.setParentId(parentId);
            comment.setAuthor(author);
            comment.setText(text + " " + i);
            comment.setCreationDate(date.plusSeconds(i));
            comments.add(comment);
        }
        return comments;
    }
}
